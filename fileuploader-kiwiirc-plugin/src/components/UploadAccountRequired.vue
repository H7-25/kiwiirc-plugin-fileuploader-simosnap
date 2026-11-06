<template>
    <div
        v-if="visible"
        class="p-fileuploader-account-overlay"
        @click.self="close"
    >
        <section
            class="p-fileuploader-account-dialog"
            role="dialog"
            aria-modal="true"
            aria-label="Caricamento riservato agli utenti identificati"
            @keydown.esc.stop.prevent="close"
            @keydown.tab.prevent="focusCloseButton"
        >
            <h2>Accedi per caricare file</h2>

            <p>
                Per condividere immagini, audio e video devi accedere
                a un account registrato.
            </p>

            <button
                ref="closeButton"
                type="button"
                @click="close"
            >
                Chiudi
            </button>
        </section>
    </div>
</template>

<script>
export default {
    data() {
        return {
            visible: false,
        };
    },

    methods: {
        open() {
            if (!this.visible) {
                this.previousFocus = document.activeElement;
            }

            this.visible = true;
            this.$nextTick(() => {
                const workspace = document.querySelector('.kiwi-workspace');

                if (workspace) {
                    const style = window.getComputedStyle(workspace);

                    this.$el.style.fontFamily = style.fontFamily;
                    this.$el.style.fontSize = style.fontSize;
                }

                this.focusCloseButton();
            });
        },

        close() {
            this.visible = false;

            const previousFocus = this.previousFocus;
            this.previousFocus = null;

            this.$nextTick(() => {
                if (
                    previousFocus &&
                    previousFocus.isConnected &&
                    typeof previousFocus.focus === 'function'
                ) {
                    previousFocus.focus();
                }
            });
        },

        focusCloseButton() {
            if (this.$refs.closeButton) {
                this.$refs.closeButton.focus();
            }
        },
    },
};
</script>

<style scoped>
.p-fileuploader-account-overlay {
    position: fixed;
    inset: 0;
    z-index: 10000;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
    background: var(--brand-shadow);
    color: var(--brand-default-fg);
    line-height: 1.5;
}

.p-fileuploader-account-dialog {
    box-sizing: border-box;
    width: 100%;
    max-width: 400px;
    max-height: 100%;
    overflow: auto;
    padding: 28px;
    border: 1px solid var(--comp-border);
    border-radius: 12px;
    background: var(--brand-default-bg);
    color: var(--brand-default-fg);
    text-align: center;
    box-shadow: 0 12px 36px var(--brand-shadow);
}

.p-fileuploader-account-dialog h2 {
    margin: 0 0 12px;
    font-family: inherit;
    font-size: 1.25em;
    font-weight: 600;
    line-height: 1.3;
}

.p-fileuploader-account-dialog p {
    margin: 0 0 24px;
    font-family: inherit;
    font-size: 1em;
    line-height: 1.6;
}

.p-fileuploader-account-dialog button {
    min-height: 42px;
    padding: 10px 24px;
    border: 1px solid var(--brand-primary);
    border-radius: 6px;
    background: var(--brand-primary);
    color: var(--brand-default-bg);
    cursor: pointer;
    font: inherit;
    font-weight: 600;
}

.p-fileuploader-account-dialog button:hover {
    border-color: var(--brand-primary-hover);
    background: var(--brand-primary-hover);
}

.p-fileuploader-account-dialog button:focus-visible {
    outline: 2px solid var(--brand-primary);
    outline-offset: 3px;
}
</style>
