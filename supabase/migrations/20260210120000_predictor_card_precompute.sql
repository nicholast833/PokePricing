-- Per-card predictor outputs (composite X, calibrated $) built by GitHub Actions.
-- The global LSRL coefficients live in ``predictor_analytics_assets`` (asset_key ``predictor_engine_snapshot``).

create table if not exists public.predictor_card_precompute (
    unique_card_id text primary key,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

comment on table public.predictor_card_precompute is
    'Precomputed predictor fields keyed by unique_card_id. Written by github_actions/precompute_predictor_from_supabase.py.';

create index if not exists idx_predictor_card_precompute_updated_at
    on public.predictor_card_precompute (updated_at desc);

alter table public.predictor_card_precompute enable row level security;

create policy "predictor_card_precompute_select_public"
    on public.predictor_card_precompute
    for select
    to anon, authenticated
    using (true);
