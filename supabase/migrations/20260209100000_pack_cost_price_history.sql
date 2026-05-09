-- Dated pack cost series (TCGGO-derived), aligned with card price_history_en shape for shared tooling.

alter table public.pokemon_set_pack_pricing
    add column if not exists pack_cost_price_history jsonb not null default '[]'::jsonb,
    add column if not exists pack_cost_price_history_en jsonb;

comment on column public.pokemon_set_pack_pricing.pack_cost_price_history is
    'Ascending daily rows: same fields as card tcggo_market_history (date, price_usd, cm_low, optional high/low/mid_usd).';

comment on column public.pokemon_set_pack_pricing.pack_cost_price_history_en is
    'Optional envelope { currency, daily{YYYY-MM-DD}, sync_iso, source } — mirrors card tcggo.price_history_en.daily.';
