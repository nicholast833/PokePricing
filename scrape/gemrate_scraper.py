import urllib.request as r
import urllib.parse
import re
import json
import time
import os
import sys
import tempfile

def normalize_set_name(s_name):
    s_name = (s_name or '').lower().replace('&', ' and ')
    s_name = s_name.replace('pokemon ', '').replace(':', '').replace('-', ' ').strip()
    while '  ' in s_name:
        s_name = s_name.replace('  ', ' ')
    return s_name


def _series_plus_name_norm(lseries, lname):
    """Avoid 'Neo' + 'Neo Revelation' -> 'neo neo revelation' (bad GemRate key collisions)."""
    ls = (lseries or '').strip()
    ln = (lname or '').strip()
    if not ln:
        return ''
    if ls and ln.lower().startswith(ls.lower() + ' '):
        return normalize_set_name(ln)
    if ls:
        return normalize_set_name(f'{ls} {ln}')
    return normalize_set_name(ln)


_OVERRIDE_STOPWORDS = frozenset(
    {'base', 'set', 'series', 'promo', 'edition', 'tcg', 'black', 'star', 'white', 'and', 'the'}
)


def _override_plausible(lname, gem_rec):
    """Reject stale/wrong grouped_sets_found rows (e.g. Neo Revelation -> Shining Fates)."""
    gl = (gem_rec.get('set_name') or '').lower()
    toks = [
        t for t in re.findall(r'[a-z0-9]+', (lname or '').lower())
        if t not in _OVERRIDE_STOPWORDS
    ]
    sig = [t for t in toks if len(t) >= 4]
    if not sig:
        sig = [t for t in toks if len(t) >= 3]
    if not sig:
        return True
    return all(t in gl for t in sig)


def _dedupe_gem_cands(cands):
    if not cands:
        return []
    seen = set()
    out = []
    for g in cands:
        sid = g.get('set_id') or g.get('set_link') or id(g)
        if sid in seen:
            continue
        seen.add(sid)
        out.append(g)
    return out


def _pick_best_gemset(candidates, lname, lseries):
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    ln = (lname or '').strip().lower()
    ls = (lseries or '').strip().lower()
    parts = [p for p in (ls, ln) if p]
    sig_toks = [t for t in re.findall(r'[a-z0-9]{4,}', ln)]

    def score(g):
        gname = (g.get('set_name') or '').lower()
        s = 0
        for p in parts:
            if p and p in gname:
                s += len(p) * 2
        for tok in ln.split():
            if len(tok) > 2 and tok not in gname:
                s -= 12
        for tok in sig_toks:
            if tok not in gname:
                s -= 28
        return s

    return max(candidates, key=score)


def _norm_card_number_for_match(num):
    if num is None:
        return ''
    s = str(num).split('/')[0].strip()
    if not s:
        return ''
    try:
        return str(int(s))
    except ValueError:
        return s.lower()


def _player_name_query_candidates(card_name):
    """GemRate player search works on base species (e.g. Gyarados-EX -> Gyarados)."""
    n = (card_name or '').strip()
    out = []
    if n:
        out.append(n)
    if '-' in n:
        a, b = n.split('-', 1)
        bup = b.strip().upper()
        if bup in ('EX', 'GX', 'V', 'VSTAR', 'VMAX', 'LEGEND'):
            a = a.strip()
            if a and a not in out:
                out.append(a)
    return out


def _year_for_adv(year_field):
    m = re.search(r'\b(19|20)\d{2}\b', str(year_field or ''))
    return int(m.group(0)) if m else None


def fetch_player_psa_rows(pokemon_name):
    """PSA checklist rows from GemRate player search (see https://www.gemrate.com/player?grader=psa&category=&player=)."""
    q = urllib.parse.quote((pokemon_name or '').strip(), safe='')
    if not q:
        return []
    url = f'https://www.gemrate.com/player?grader=psa&category=&player={q}'
    req = r.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        html = r.urlopen(req, timeout=90).read().decode('utf-8', 'replace')
    except Exception:
        return []
    m = re.search(r"var RowData = JSON\.parse\('(.*?)'\)\s*;", html, re.DOTALL)
    if not m:
        return []
    try:
        json_str = m.group(1).replace("\\'", "'")
        return json.loads(json_str)
    except json.JSONDecodeError:
        return []


def _gem_record_for_player_set_title(gemrate_sets, player_set_title):
    nt = normalize_set_name(player_set_title)
    for g in gemrate_sets:
        if g.get('category') != 'TCG':
            continue
        if normalize_set_name(g.get('set_name', '')) == nt:
            return g
    return None


def _resolve_via_player_checklist(lset, gemrate_sets, gem_exact_dict, player_rows_cache):
    """
    When Universal Pop `setsData` has no row for a short local title, use the top card's name
    on GemRate's PSA player checklist; the correct expansion set almost always appears there.
    """
    cards = lset.get('top_25_cards') or []
    if not cards:
        return None
    top = cards[0]
    cname = top.get('name') or ''
    local_num = _norm_card_number_for_match(top.get('number'))
    lname = lset.get('set_name', '')
    lseries = lset.get('series', '')
    full_local_norm = normalize_set_name(_series_plus_name_norm(lseries, lname))
    toks = [
        t for t in re.findall(r'[a-z0-9]+', (lname or '').lower())
        if t not in _OVERRIDE_STOPWORDS
    ]
    sig = [t for t in toks if len(t) >= 4]
    if not sig:
        sig = [t for t in toks if len(t) >= 3]
    if not sig:
        sig = [t for t in toks if len(t) >= 2]

    rows = []
    for pname in _player_name_query_candidates(cname):
        if pname not in player_rows_cache:
            time.sleep(0.2)
            player_rows_cache[pname] = fetch_player_psa_rows(pname)
        rows = player_rows_cache[pname]
        if rows:
            break
    if not rows:
        return None

    rows = [x for x in rows if str(x.get('category') or '').upper() in ('', 'TCG')]
    if not rows:
        return None

    non_pl = [x for x in rows if 'polish' not in str(x.get('set_name', '')).lower()]
    if non_pl:
        rows = non_pl

    if local_num:
        narrowed = [x for x in rows if _norm_card_number_for_match(x.get('card_number')) == local_num]
        if narrowed:
            rows = narrowed

    scored = []
    seen_sn = set()
    for row in rows:
        sn = str(row.get('set_name') or '').strip()
        if not sn or sn in seen_sn:
            continue
        seen_sn.add(sn)
        ns = normalize_set_name(sn)
        if sig:
            score = sum(1 for t in sig if t in ns)
        else:
            score = 2 if full_local_norm and full_local_norm in ns else 0
        scored.append((score, sn, row))

    scored.sort(key=lambda x: (-x[0], len(x[1])))
    if not scored:
        return None

    best_score = scored[0][0]
    if best_score <= 0:
        return None

    tier = [x for x in scored if x[0] == best_score]
    for _score, sn, row in tier:
        gem_rec = _gem_record_for_player_set_title(gemrate_sets, sn)
        if gem_rec:
            return gem_rec
        if sn in gem_exact_dict:
            return gem_exact_dict[sn]

    _bs, sn, row = tier[0]
    y = _year_for_adv(row.get('year'))
    if not y:
        return None
    return {
        'set_name': sn,
        'category': 'TCG',
        'set_link': _build_advanced_pop_report_url(y, sn),
        'set_id': '',
        'total_grades': 0,
        'is_advanced': True,
    }


def _build_advanced_pop_report_url(year, set_name_psa):
    qs = urllib.parse.urlencode(
        {
            'grader': 'psa',
            'category': 'tcg-cards',
            'year': str(year),
            'set_name': set_name_psa,
        },
        quote_via=urllib.parse.quote,
    )
    return f'https://www.gemrate.com/item-details-advanced?{qs}'


# Universal Pop `setsData` omits some WOTC/e-card rows; PSA Advanced pop pages still exist.
_ADV_POP_BY_SET_CODE = {
    'gym1': (2000, 'Pokemon Gym Heroes'),
    'neo3': (2001, 'Pokemon Neo Revelation'),
    'base6': (2002, 'Pokemon Legendary Collection'),
    'ecard1': (2002, 'Pokemon Expedition'),
    # Mega Evolution EN — Universal Pop list often lags; PSA Advanced pages (set_name must match GemRate).
    'me2pt5': (2026, 'Pokemon Asc EN-Ascended Heroes'),
    'me3': (2026, 'Pokemon Por EN-Perfect Order'),
}

# Short local titles -> exact GemRate universal `set_name` keys from setsData
_UNIVERSAL_EXACT_BY_SET_CODE = {
    'base1': 'Pokemon Base Set',
}


def map_set_names(local_sets, gemrate_sets, overrides, trusted_override_names=None):
    mapping = {}
    player_rows_cache = {}
    gem_exact_dict = {s['set_name']: s for s in gemrate_sets if s.get('category') == 'TCG'}
    # Normalized key -> list (GemRate can have collisions if we only keep one dict entry)
    def _alias_keys(norm_key):
        if not norm_key:
            return []
        out = [norm_key]
        stripped = re.sub(r'^\d{4}\s+', '', norm_key).strip()
        if stripped and stripped not in out:
            out.append(stripped)
        return out

    gem_by_norm = {}
    for s in gemrate_sets:
        if s.get('category') != 'TCG':
            continue
        k = normalize_set_name(s['set_name'])
        extra_suffix = []
        if 'sword and shield' in k or 'scarlet and violet' in k:
            toks = k.split()
            if len(toks) >= 2:
                extra_suffix.append(' '.join(toks[-2:]))
            if len(toks) >= 3:
                extra_suffix.append(' '.join(toks[-3:]))
        seen_alias = set()
        for ak in _alias_keys(k) + extra_suffix:
            if not ak or ak in seen_alias:
                continue
            seen_alias.add(ak)
            gem_by_norm.setdefault(ak, []).append(s)

    trusted = trusted_override_names or frozenset()

    for lset in local_sets:
        lname = lset.get('set_name', '')
        lseries = lset.get('series', '')

        # Check explicit overrides first
        if lname in overrides:
            val = overrides[lname]
            if val.startswith('[ADV]'):
                qs = val.replace('[ADV]', '').strip()
                # Parse the query string dict and encode values properly (including internal ampersands)
                parsed = urllib.parse.parse_qsl(qs)
                encoded_qs = urllib.parse.urlencode(parsed, quote_via=urllib.parse.quote)
                adv_url = f"https://www.gemrate.com/item-details-advanced?grader=psa&category=tcg-cards&{encoded_qs}"
                mapping[lset['set_code']] = {'set_link': adv_url, 'is_advanced': True}
                continue
            if val in gem_exact_dict:
                gem_rec = gem_exact_dict[val]
                if lname in trusted or _override_plausible(lname, gem_rec):
                    mapping[lset['set_code']] = gem_rec
                    continue

        norm_1 = _series_plus_name_norm(lseries, lname)
        norm_2 = normalize_set_name(lname)
        norm_3 = normalize_set_name(f'{lseries}  {lname}')

        def _norm_lookup_chain(*keys):
            seen_k = set()
            for k in keys:
                if not k or k in seen_k:
                    continue
                seen_k.add(k)
                yield k
                stripped = re.sub(r'^\d{4}\s+', '', k).strip()
                if stripped and stripped != k and stripped not in seen_k:
                    seen_k.add(stripped)
                    yield stripped

        found_gem = None
        # Prefer bare set name key, then series+name (deduped), then spaced variant
        for nk in _norm_lookup_chain(norm_2, norm_1, norm_3):
            cands = _dedupe_gem_cands(gem_by_norm.get(nk) or [])
            if not cands:
                continue
            if len(cands) == 1:
                found_gem = cands[0]
            else:
                found_gem = _pick_best_gemset(cands, lname, lseries)
            if found_gem:
                break

        if not found_gem:
            code = lset['set_code']
            adv = _ADV_POP_BY_SET_CODE.get(code)
            if adv:
                y, sn = adv
                found_gem = {
                    'set_name': sn,
                    'category': 'TCG',
                    'set_link': _build_advanced_pop_report_url(y, sn),
                    'set_id': '',
                    'total_grades': 0,
                    'is_advanced': True,
                }
            else:
                ex = _UNIVERSAL_EXACT_BY_SET_CODE.get(code)
                if ex and ex in gem_exact_dict:
                    found_gem = gem_exact_dict[ex]

        if not found_gem:
            found_gem = _resolve_via_player_checklist(
                lset, gemrate_sets, gem_exact_dict, player_rows_cache
            )

        if found_gem:
            mapping[lset['set_code']] = found_gem
    return mapping

def fetch_gemrate_sets():
    """Load TCG rows from Universal Pop `setsData` (subset of PSA checklist; not the player card grid)."""
    print("Fetching Gemrate master list...")
    url = 'https://www.gemrate.com/universal-pop-report'
    req = r.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = r.urlopen(req).read().decode('utf-8')
    match = re.search(r'setsData\s*=\s*(\[.*?\])\s*;', html, re.DOTALL)
    if not match: match = re.search(r'let setsData = (\[.*?\]);', html, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return []

def fetch_set_cards(set_link):
    print(f"Fetching {set_link}...")
    req = r.Request(set_link, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        html = r.urlopen(req).read().decode('utf-8')
        # Advanced pop (player-linked URLs): var RowData = JSON.parse('...');
        m_adv_jp = re.search(r"var RowData = JSON\.parse\('(.*?)'\)\s*;", html, re.DOTALL)
        if m_adv_jp:
            json_str = m_adv_jp.group(1).replace("\\'", "'")
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

        # Universal Pop Report schema
        match = re.search(r'rowData\s*=\s*JSON\.parse\(\'(.*?)\'\);', html, re.DOTALL)
        if match:
            json_str = match.group(1).replace("\\'", "'")
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        # Universal Pop Report schema array
        match2 = re.search(r'rowData\s*=\s*(\[.*?\])\s*;\s*\n', html, re.DOTALL)
        if match2:
            return json.loads(match2.group(1))

        # Advanced Pop Report: quoted RowData (escaped inner quotes)
        for _rd_pat in (
            r"(?:var|let|const)\s+RowData\s*=\s*'((?:[^'\\]|\\.)*)'\s*;",
            r"var RowData\s*=\s*'((?:[^'\\]|\\.)*)'\s*;",
            r"RowData\s*=\s*'((?:[^'\\]|\\.)*)'\s*;",
        ):
            match3b = re.search(_rd_pat, html, re.DOTALL)
            if match3b:
                json_str = match3b.group(1).replace("\\'", "'")
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass

        # Advanced Pop Report schema string (no var)
        match3 = re.search(r'RowData\s*=\s*\'(.*?)\'\s*;', html, re.DOTALL)
        if match3:
            json_str = match3.group(1).replace("\\'", "'")
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

        # Advanced Pop Report schema array
        match4 = re.search(r'RowData\s*=\s*(\[.*?\])\s*;', html)
        if match4:
            return json.loads(match4.group(1))

            
    except Exception as e:
        print(f"Failed to fetch or parse {set_link}: {e}")
    return []


def _normalize_gem_card_row(gc):
    """PSA Advanced `item-details` rows use `card_*` fields; Universal Pop uses `total_*` / `psa_*`."""
    if not isinstance(gc, dict):
        return gc
    if gc.get('total_grades') is not None:
        return gc
    if gc.get('card_total_grades') is None and gc.get('card_gems') is None:
        return gc
    out = dict(gc)
    ctg = out.get('card_total_grades')
    cgems = out.get('card_gems')
    out['total_grades'] = ctg
    out['total_gem_mint'] = cgems
    out['total_gems'] = cgems
    out['total_gem_rate'] = out.get('card_gem_rate')
    out['psa_gems'] = cgems
    out['psa_card_total_grades'] = ctg
    out['psa_card_gem_rate'] = out.get('card_gem_rate')
    return out


def _extract_card_details_token(html):
    m = re.search(r'const cardDetailsToken = "([^"]+)"', html or '')
    return m.group(1) if m else None


def _fetch_card_details_json(gemrate_id, token):
    """Universal merged pop per card (PSA/BGS/CGC/SGC). Requires token from universal-search HTML."""
    if not gemrate_id or not token:
        return None
    qid = urllib.parse.quote(str(gemrate_id).strip(), safe='')
    url = f'https://www.gemrate.com/card-details?gemrate_id={qid}'
    req = r.Request(
        url,
        headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json, text/plain, */*',
            'X-Card-Details-Token': token,
            'Referer': 'https://www.gemrate.com/',
        },
    )
    try:
        raw = r.urlopen(req, timeout=45).read().decode('utf-8')
        return json.loads(raw)
    except Exception as e:
        print(f'  card-details failed for {gemrate_id[:12]}…: {e}')
        return None


def _gemrate_from_card_details(cd):
    """Map GemRate /card-details JSON into our card.gemrate shape."""
    if not cd or not isinstance(cd, dict):
        return None

    def _i(v):
        if v is None:
            return 0
        try:
            n = int(float(v))
            return n if n >= 0 else 0
        except (TypeError, ValueError):
            return 0

    def _rate(v):
        if v is None:
            return None
        try:
            x = float(v)
            return x if x == x else None
        except (TypeError, ValueError):
            return None

    pop_list = cd.get('population_data') or []
    pop = {str(x.get('grader', '')).lower(): x for x in pop_list if isinstance(x, dict)}
    psa = pop.get('psa') or {}
    bgs = pop.get('beckett') or {}
    cgc = pop.get('cgc') or {}
    sgc = pop.get('sgc') or {}

    total = _i(cd.get('total_population'))
    total_gems = _i(cd.get('total_gems_or_greater'))
    total_rate = (total_gems / total) if total > 0 else _rate(cd.get('total_gem_rate'))

    return {
        'total': total,
        'total_gem_mint': total_gems,
        'total_gem_rate': total_rate,
        'psa_gems': _i(psa.get('card_gems')),
        'beckett_gems': _i(bgs.get('card_gems')),
        'cgc_gems': _i(cgc.get('card_gems')),
        'sgc_gems': _i(sgc.get('card_gems')),
        'psa_grades': _i(psa.get('card_total_grades')),
        'cgc_grades': _i(cgc.get('card_total_grades')),
        'beckett_grades': _i(bgs.get('card_total_grades')),
        'sgc_grades': _i(sgc.get('card_total_grades')),
        'psa_gem_rate': _rate(psa.get('card_gem_rate')),
        'cgc_gem_rate': _rate(cgc.get('card_gem_rate')),
        'beckett_gem_rate': _rate(bgs.get('card_gem_rate')),
        'sgc_gem_rate': _rate(sgc.get('card_gem_rate')),
    }


def _load_local_sets(path='pokemon_sets_data.json'):
    with open(path, 'r', encoding='utf-8') as f:
        raw = f.read()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        obj, _end = json.JSONDecoder().raw_decode(raw)
        return obj


def _load_gemrate_overrides():
    overrides = {}
    trusted_override_names = frozenset()
    local_ov = os.path.join(os.path.dirname(__file__), 'gemrate_set_overrides.json')
    if os.path.exists(local_ov):
        with open(local_ov, 'r', encoding='utf-8') as f:
            overrides = json.load(f)
        trusted_override_names = frozenset(overrides.keys())
    ext_paths = (
        os.path.join(os.path.dirname(__file__), '.gemini', 'antigravity', 'brain', 'd1385aae-cb44-4cfe-87ff-1ed0eb6982d0', 'scratch', 'grouped_sets_found.json'),
        r'C:\Users\slend\.gemini\antigravity\brain\d1385aae-cb44-4cfe-87ff-1ed0eb6982d0\scratch\grouped_sets_found.json',
    )
    for p in ext_paths:
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as f:
                ext = json.load(f)
            for k, v in ext.items():
                overrides.setdefault(k, v)
            break
    return overrides, trusted_override_names


def write_missing_gemrate_report_temp():
    """Write sets with no GemRate mapping or no card-level pop data to a temp .txt; return path."""
    data_path = os.path.join(os.path.dirname(__file__), 'pokemon_sets_data.json')
    local_sets = _load_local_sets(data_path)
    overrides, trusted_override_names = _load_gemrate_overrides()
    gem_sets = fetch_gemrate_sets()
    mapped_sets = map_set_names(
        local_sets, gem_sets, overrides, trusted_override_names=trusted_override_names
    )
    rows = []
    for s in local_sets:
        code = s['set_code']
        name = s.get('set_name', '')
        if code not in mapped_sets:
            rows.append((code, name, 'no_mapping', ''))
            continue
        link = (mapped_sets[code].get('set_link') or '').strip()
        cards = s.get('top_25_cards') or []
        has_pop = any(
            (c.get('gemrate') or {}).get('total') is not None
            for c in cards
        )
        if not has_pop:
            rows.append((code, name, 'mapped_no_card_pop', link))

    fd, path = tempfile.mkstemp(prefix='gemrate_missing_', suffix='.txt', text=True)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write(
            '# Sets with missing GemRate coverage (no heuristic mapping, or mapped but no card pop in JSON)\n'
        )
        f.write(f'# count={len(rows)}\n')
        f.write('# set_code\tset_name\treason\tgemrate_set_link\n')
        for code, name, reason, link in rows:
            f.write(f'{code}\t{name}\t{reason}\t{link}\n')
    return path


def _atomic_write_json(path, data, indent=4):
    d = os.path.dirname(os.path.abspath(path)) or '.'
    fd, tmp = tempfile.mkstemp(prefix='pokemon_sets_', suffix='.json.tmp', dir=d)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def run_scraper(target_sets=None):
    data_path = os.path.join(os.path.dirname(__file__), 'pokemon_sets_data.json')
    gem_sets = fetch_gemrate_sets()

    local_sets = _load_local_sets(data_path)

    overrides, trusted_override_names = _load_gemrate_overrides()

    mapped_sets = map_set_names(
        local_sets, gem_sets, overrides, trusted_override_names=trusted_override_names
    )
    print(f"Mapped {len(mapped_sets)} out of {len(local_sets)} local sets")

    not_mapped = []
    fetch_failed = []

    count_scraped = 0
    for lset in local_sets:
        set_code = lset['set_code']
        if set_code not in mapped_sets:
            not_mapped.append(
                (set_code, lset.get('set_name', ''), 'no GemRate set mapping')
            )
            if target_sets and any(ts in lset.get('set_name', '') for ts in target_sets):
                print(f"Set '{lset.get('set_name')}' not mapped to Gemrate! (No data available)")
            continue

        if target_sets and not any(ts in lset.get('set_name', '') for ts in target_sets):
            continue

        gem_set_data = mapped_sets[set_code]
        set_link = gem_set_data.get('set_link')

        # Attach set level pop to the list
        lset['gemrate_set_total'] = gem_set_data.get('total_grades', 0)
        lset['gemrate_id'] = gem_set_data.get('set_id', '')
        _sl = gem_set_data.get('set_link') or ''
        lset['gemrate_set_link'] = str(_sl).strip()

        cards_data = fetch_set_cards(set_link)
        if not cards_data:
            fetch_failed.append(
                (set_code, lset.get('set_name', ''), 'mapped but empty/unparseable page', set_link or '')
            )
            time.sleep(2)
            continue

        print(f"Found {len(cards_data)} cards for {lset['set_name']}")

        is_advanced = 'item-details-advanced' in (set_link or '').lower()
        card_details_token = None
        if is_advanced:
            seed_gid = next(
                (
                    str((r.get('universal_gemrate_id') or r.get('gemrate_id') or '')).strip()
                    for r in cards_data
                    if isinstance(r, dict) and (r.get('universal_gemrate_id') or r.get('gemrate_id'))
                ),
                '',
            )
            if seed_gid:
                try:
                    us_url = f'https://www.gemrate.com/universal-search?gemrate_id={urllib.parse.quote(seed_gid)}'
                    us_html = r.urlopen(
                        r.Request(us_url, headers={'User-Agent': 'Mozilla/5.0'}),
                        timeout=45,
                    ).read().decode('utf-8')
                    card_details_token = _extract_card_details_token(us_html)
                    if not card_details_token:
                        print('  Warning: universal-search page had no cardDetailsToken')
                except Exception as e:
                    print(f'  Warning: could not load card-details token: {e}')

        if not lset.get('gemrate_id'):
            def _row_graded_total(row):
                if not isinstance(row, dict):
                    return 0
                for key in ('card_total_grades', 'total_grades'):
                    v = row.get(key)
                    if v is None:
                        continue
                    try:
                        n = int(float(v))
                        return n if n >= 0 else 0
                    except (TypeError, ValueError):
                        continue
                return 0

            adv_sum = sum(_row_graded_total(r) for r in cards_data)
            if adv_sum > 0:
                lset['gemrate_set_total'] = adv_sum

        card_details_cache = {}

        def _int_pop(v):
            if v is None:
                return 0
            try:
                n = int(float(v))
                return n if n >= 0 else 0
            except (TypeError, ValueError):
                return 0

        def _rate_pop(v):
            if v is None:
                return None
            try:
                x = float(v)
                return x if x == x else None
            except (TypeError, ValueError):
                return None

        for card in lset.get('top_25_cards', []):
            card_num = card.get('number', '').split('/')[0].strip()
            # Match card logic: match by card_number and fuzzy name
            gem_card = None
            for gc in cards_data:
                gc_num = str(gc.get('card_number', '')).strip()
                if gc_num == card_num:
                    gem_card = gc
                    break

            if gem_card:
                gem_card = _normalize_gem_card_row(gem_card)
                gid = str(gem_card.get('universal_gemrate_id') or gem_card.get('gemrate_id') or '').strip()
                merged = None
                if is_advanced and card_details_token and gid:
                    if gid not in card_details_cache:
                        card_details_cache[gid] = _fetch_card_details_json(gid, card_details_token)
                        time.sleep(0.15)
                    cd = card_details_cache.get(gid)
                    if cd:
                        merged = _gemrate_from_card_details(cd)
                if merged:
                    card['gemrate'] = merged
                else:
                    card['gemrate'] = {
                        'total': _int_pop(gem_card.get('total_grades')),
                        'total_gem_mint': _int_pop(gem_card.get('total_gem_mint') or gem_card.get('total_gems')),
                        'total_gem_rate': _rate_pop(gem_card.get('total_gem_rate')),
                        'psa_gems': _int_pop(gem_card.get('psa_gems')),
                        'beckett_gems': _int_pop(gem_card.get('beckett_gems')),
                        'cgc_gems': _int_pop(gem_card.get('cgc_gems')),
                        'sgc_gems': _int_pop(gem_card.get('sgc_gems')),
                        'psa_grades': _int_pop(gem_card.get('psa_card_total_grades')),
                        'cgc_grades': _int_pop(gem_card.get('cgc_card_total_grades')),
                        'beckett_grades': _int_pop(gem_card.get('beckett_card_total_grades')),
                        'sgc_grades': _int_pop(gem_card.get('sgc_card_total_grades')),
                        'psa_gem_rate': _rate_pop(gem_card.get('psa_card_gem_rate')),
                        'cgc_gem_rate': _rate_pop(gem_card.get('cgc_card_gem_rate')),
                        'beckett_gem_rate': _rate_pop(gem_card.get('beckett_card_gem_rate')),
                        'sgc_gem_rate': _rate_pop(gem_card.get('sgc_card_gem_rate')),
                    }
            else:
                card['gemrate'] = None

        count_scraped += 1
        time.sleep(2)

    report_path = os.path.join(os.path.dirname(__file__), 'gemrate_scrape_skipped.txt')
    lines = [
        f"# GemRate scrape report ({len(not_mapped)} not mapped, {len(fetch_failed)} fetch failures)",
        '# not mapped:',
    ]
    for row in not_mapped:
        lines.append(f"{row[0]}\t{row[1]}\t{row[2]}")
    lines.append('')
    lines.append('# mapped but no card table parsed:')
    for row in fetch_failed:
        lines.append(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f"Wrote {report_path}")

    # Write back (atomic replace so partial writes cannot corrupt the main DB)
    print("Writing mapping to disk...")
    _atomic_write_json(data_path, local_sets)

    print(f"Successfully scraped {count_scraped} sets.")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ('--missing-gemrate-report', '--report-missing-gemrate'):
        out = write_missing_gemrate_report_temp()
        print(out)
    elif len(sys.argv) > 1:
        # e.g. python scrape/gemrate_scraper.py "Ascended Heroes" "Perfect Order"
        run_scraper(target_sets=sys.argv[1:])
    else:
        run_scraper(target_sets=None)
