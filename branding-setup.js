// Central Configuration for All Widgets
// Edit this file to change styles across ALL widgets

const WIDGET_CONFIG = {
    // Font Settings
    fontFamily: "'Segoe UI', Arial, sans-serif",
    fontSize: {
        value: 42,                    // Main number size
        label: 16                     // Label text size
    },
    fontWeight: "bold",

    // Colors
    colors: {
        primary: "#9146ff",         // Default (subscribers, bits)
        monthly: "#ff6b6b",         // Artist monthly
        yearly: "#ffd700",          // Artist total
        label: "#000",              // Label text
        textShadow: "rgba(0,0,0,0.8)"
    },

    // Background
    background: {
        enabled: false,               // Set to true for colored background
        color: "rgba(0,0,0,0.3)",
        borderRadius: 10
    },

    // Border
    border: {
        width: 0,
        style: "solid"
    },

    // Layout
    layout: {
        padding: "5px 5px",
        gap: 5,
        labelPosition: "top"           // "bottom" or "top"
    }
};

// Apply styles to DOM
function applyWidgetStyles() {
    const root = document.documentElement;

    // Font settings
    root.style.setProperty('--font-family', WIDGET_CONFIG.fontFamily);
    root.style.setProperty('--font-size', WIDGET_CONFIG.fontSize.value + 'px');
    root.style.setProperty('--font-size-label', WIDGET_CONFIG.fontSize.label + 'px');
    root.style.setProperty('--font-weight', WIDGET_CONFIG.fontWeight);

    // Colors
    root.style.setProperty('--color-primary', WIDGET_CONFIG.colors.primary);
    root.style.setProperty('--color-monthly', WIDGET_CONFIG.colors.monthly);
    root.style.setProperty('--color-yearly', WIDGET_CONFIG.colors.yearly);
    root.style.setProperty('--color-label', WIDGET_CONFIG.colors.label);
    root.style.setProperty('--text-shadow', `2px 2px 4px ${WIDGET_CONFIG.colors.textShadow}`);

    // Background
    if (WIDGET_CONFIG.background.enabled) {
        root.style.setProperty('--bg-color', WIDGET_CONFIG.background.color);
        root.style.setProperty('--border-radius', WIDGET_CONFIG.background.borderRadius + 'px');
    } else {
        root.style.setProperty('--bg-color', 'transparent');
        root.style.setProperty('--border-radius', '0');
    }

    // Border
    root.style.setProperty('--border-width', WIDGET_CONFIG.border.width + 'px');
    root.style.setProperty('--border-style', WIDGET_CONFIG.border.style);

    // Layout
    root.style.setProperty('--padding', WIDGET_CONFIG.layout.padding);
    root.style.setProperty('--gap', WIDGET_CONFIG.layout.gap + 'px');

    // Label position (1 = value, 2 = label for "bottom", swap for "top")
    const valueOrder = WIDGET_CONFIG.layout.labelPosition === "top" ? 2 : 1;
    const labelOrder = WIDGET_CONFIG.layout.labelPosition === "top" ? 1 : 2;
    root.style.setProperty('--value-order', valueOrder);
    root.style.setProperty('--label-order', labelOrder);
}

// Initialize
document.addEventListener('DOMContentLoaded', applyWidgetStyles);
