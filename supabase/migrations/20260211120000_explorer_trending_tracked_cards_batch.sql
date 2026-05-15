-- Slim batch fetch for github_actions/build_explorer_trending_from_supabase.py:
-- avoids downloading full metrics + price_history (statement timeout on large JSON).

create index if not exists pokemon_cards_tracked_priority_idx
    on public.pokemon_cards (tracked_priority desc, unique_card_id)
    where (tracked_priority is not null and tracked_priority >= 1);

create or replace function public.explorer_trending_tracked_cards_batch(p_offset int, p_limit int)
returns table (
    unique_card_id text,
    set_code text,
    name text,
    number text,
    image_url text,
    rarity text,
    market_price double precision,
    tracked_priority integer,
    tcggo_ebay_sold_prices jsonb,
    collectrics_history_justtcg jsonb,
    tcggo_market_history jsonb
)
language sql
stable
set search_path = public
as $$
    select
        c.unique_card_id::text,
        c.set_code::text,
        coalesce(c.name, '')::text,
        coalesce(c.number::text, '')::text,
        coalesce(c.image_url, '')::text,
        coalesce(c.rarity, '')::text,
        c.market_price::double precision,
        c.tracked_priority::integer,
        case
            when coalesce(c.metrics, '{}'::jsonb) ? 'tcggo_ebay_sold_prices' then c.metrics -> 'tcggo_ebay_sold_prices'
            else 'null'::jsonb
        end as tcggo_ebay_sold_prices,
        case
            when coalesce(c.metrics, '{}'::jsonb) ? 'collectrics_history_justtcg' then c.metrics -> 'collectrics_history_justtcg'
            else 'null'::jsonb
        end as collectrics_history_justtcg,
        case
            when coalesce(c.price_history, '{}'::jsonb) ? 'tcggo_market_history' then c.price_history -> 'tcggo_market_history'
            else '[]'::jsonb
        end as tcggo_market_history
    from public.pokemon_cards c
    where c.tracked_priority is not null
      and c.tracked_priority >= 1
    order by c.tracked_priority desc, c.unique_card_id
    offset greatest(p_offset, 0)
    limit least(greatest(p_limit, 1), 500);
$$;

comment on function public.explorer_trending_tracked_cards_batch(int, int) is
    'Returns tracked cards with only JSON slices needed for explorer_trending_daily (TCGGO market hist, JustTCG hist, eBay sold).';

revoke all on function public.explorer_trending_tracked_cards_batch(int, int) from public;
grant execute on function public.explorer_trending_tracked_cards_batch(int, int) to service_role;
