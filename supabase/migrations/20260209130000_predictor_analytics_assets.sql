-- Bundled analytics sidecars (formerly data/assets/*.json on static hosting).
-- Read by the Predictor / Analytics pages via Supabase REST; written by GitHub Actions.

create table if not exists public.predictor_analytics_assets (
    asset_key text primary key,
    payload jsonb not null,
    updated_at timestamptz not null default now()
);

comment on table public.predictor_analytics_assets is
    'Keys: character_premium_scores, google_trends_momentum, artist_scores, tcg_macro_interest_by_year — same JSON shapes as repo data/assets files.';

create index if not exists idx_predictor_analytics_assets_updated_at
    on public.predictor_analytics_assets (updated_at desc);

alter table public.predictor_analytics_assets enable row level security;

create policy "predictor_analytics_assets_select_public"
    on public.predictor_analytics_assets
    for select
    to anon, authenticated
    using (true);
