// Central Configuration for All Widgets
// Edit this file to change styles across ALL widgets

const DEFAULT_CONFIG = {
    fontFamily: "'Segoe UI', Arial, sans-serif",
    fontSize: {
        value: 42,
        label: 16,
        sidebar2: {
            followers: 48,
            monthly: 42,
            lifetime: 36
        }
    },
    fontWeight: "bold",
    colors: {
        primary: "#9146ff",
        monthly: "#ff6b6b",
        yearly: "#ffd700",
        label: "#aaaaaa",
        textShadow: "rgba(0,0,0,0.8)"
    },
    background: {
        enabled: false,
        color: "rgba(0,0,0,0.3)",
        borderRadius: 10
    },
    border: {
        width: 0,
        style: "solid"
    },
    layout: {
        padding: "5px 5px",
        gap: 5,
        labelPosition: "top"
    }
};

let WIDGET_CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));

function loadBrandingConfig() {
    try {
        const stored = localStorage.getItem('brandingConfig');
        if (stored) {
            WIDGET_CONFIG = JSON.parse(stored);
        } else {
            WIDGET_CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
        }
    } catch (e) {
        WIDGET_CONFIG = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
    }
}

function applyWidgetStyles() {
    loadBrandingConfig();
    const root = document.documentElement;

    root.style.setProperty('--font-family', WIDGET_CONFIG.fontFamily);
    root.style.setProperty('--font-size', WIDGET_CONFIG.fontSize.value + 'px');
    root.style.setProperty('--font-size-label', WIDGET_CONFIG.fontSize.label + 'px');
    root.style.setProperty('--font-weight', WIDGET_CONFIG.fontWeight);

    // Sidebar2 specific font sizes
    const sidebar2Fonts = WIDGET_CONFIG.fontSize.sidebar2 || { followers: 48, monthly: 42, lifetime: 36 };
    root.style.setProperty('--font-size-followers', sidebar2Fonts.followers + 'px');
    root.style.setProperty('--font-size-monthly', sidebar2Fonts.monthly + 'px');
    root.style.setProperty('--font-size-lifetime', sidebar2Fonts.lifetime + 'px');

    root.style.setProperty('--color-primary', WIDGET_CONFIG.colors.primary);
    root.style.setProperty('--color-monthly', WIDGET_CONFIG.colors.monthly);
    root.style.setProperty('--color-yearly', WIDGET_CONFIG.colors.yearly);
    root.style.setProperty('--color-lifetime', WIDGET_CONFIG.colors.yearly);
    root.style.setProperty('--color-label', WIDGET_CONFIG.colors.label);
    root.style.setProperty('--text-shadow', `2px 2px 4px ${WIDGET_CONFIG.colors.textShadow}`);

    if (WIDGET_CONFIG.background.enabled) {
        root.style.setProperty('--bg-color', WIDGET_CONFIG.background.color);
        root.style.setProperty('--border-radius', WIDGET_CONFIG.background.borderRadius + 'px');
    } else {
        root.style.setProperty('--bg-color', 'transparent');
        root.style.setProperty('--border-radius', '0');
    }

    root.style.setProperty('--border-width', WIDGET_CONFIG.border.width + 'px');
    root.style.setProperty('--border-style', WIDGET_CONFIG.border.style);

    const valueOrder = WIDGET_CONFIG.layout.labelPosition === "top" ? 2 : 1;
    const labelOrder = WIDGET_CONFIG.layout.labelPosition === "top" ? 1 : 2;
    root.style.setProperty('--value-order', valueOrder);
    root.style.setProperty('--label-order', labelOrder);
}

document.addEventListener('DOMContentLoaded', applyWidgetStyles);
