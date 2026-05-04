/**
 * Global light / dark appearance (localStorage + html[data-ptcg-theme="light"]).
 * See tmp/prompt Phase 3; chart axis colors Phase 4 (CSS vars read by SHARED_UTILS.getChartAxisColors).
 */
(function () {
    const STORAGE_KEY = 'ptcg-theme';

    function readTheme() {
        try {
            return localStorage.getItem(STORAGE_KEY) === 'light' ? 'light' : 'dark';
        } catch (e) {
            return 'dark';
        }
    }

    function writeTheme(theme) {
        try {
            localStorage.setItem(STORAGE_KEY, theme);
        } catch (e) { /* ignore */ }
    }

    function applyTheme(theme) {
        const t = theme === 'light' ? 'light' : 'dark';
        if (t === 'light') {
            document.documentElement.setAttribute('data-ptcg-theme', 'light');
        } else {
            document.documentElement.removeAttribute('data-ptcg-theme');
        }
        writeTheme(t);
        window.dispatchEvent(new CustomEvent('ptcg-theme-changed', { detail: { theme: t } }));
    }

    function syncToggleLabels() {
        const light = document.documentElement.getAttribute('data-ptcg-theme') === 'light';
        document.querySelectorAll('[data-ptcg-theme-toggle]').forEach((btn) => {
            btn.setAttribute('aria-pressed', light ? 'true' : 'false');
            btn.textContent = light ? 'Dark mode' : 'Light mode';
            btn.title = light ? 'Switch to dark appearance' : 'Switch to light appearance';
        });
    }

    function bindToggle(btn) {
        btn.addEventListener('click', () => {
            const next = document.documentElement.getAttribute('data-ptcg-theme') === 'light' ? 'dark' : 'light';
            applyTheme(next);
        });
    }

    applyTheme(readTheme());

    function init() {
        document.querySelectorAll('[data-ptcg-theme-toggle]').forEach(bindToggle);
        syncToggleLabels();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    window.addEventListener('storage', (e) => {
        if (e.key !== STORAGE_KEY) return;
        applyTheme(readTheme());
        syncToggleLabels();
    });

    window.addEventListener('ptcg-theme-changed', syncToggleLabels);
})();
