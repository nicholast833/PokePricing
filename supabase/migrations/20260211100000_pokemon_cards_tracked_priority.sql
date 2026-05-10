-- Per-set “tracked” slice used by Explorer / Predictor / Analytics embeds (top-N by TCGGO list price).
-- ``github_actions/refresh_tcggo_tracked_top25.py`` assigns ranks 1..N; ``null`` = not in the tracked slice.

alter table public.pokemon_cards
    add column if not exists tracked_priority smallint null;

comment on column public.pokemon_cards.tracked_priority is
    '1 = most expensive in set (TCGGO ``price_highest``), ascending; null = not in tracked top slice.';

create index if not exists idx_pokemon_cards_set_tracked
    on public.pokemon_cards (set_code, tracked_priority)
    where tracked_priority is not null;
