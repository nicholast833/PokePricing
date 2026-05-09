-- Per-set pack / sealed pricing snapshot (written by github_actions sync_pack_costs + bridge apply-pack-costs).
-- Apply in Supabase: SQL Editor → paste → Run, or: supabase db push / migration pipeline.

create table if not exists public.pokemon_set_pack_pricing (
    set_code text primary key,
    synced_at timestamptz not null default now(),
    tcgplayer_pack_price numeric,
    pack_cost_primary_usd numeric,
    pack_cost_method text,
    pack_cost_sync_iso text,
    pack_cost_breakdown jsonb not null default '{}'::jsonb,
    tcgplayer_booster_pack_product_id bigint
);

comment on table public.pokemon_set_pack_pricing is
    'Latest pack-cost run per set_code: mirrors pokemon_sets.metadata pack fields + full breakdown JSON.';

create index if not exists idx_pokemon_set_pack_pricing_synced_at
    on public.pokemon_set_pack_pricing (synced_at desc);
