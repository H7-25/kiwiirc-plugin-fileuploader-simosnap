<template>
    <div
        v-if="!accountOnly || currentUser.account"
        class="p-fileuploader-upload-button"
        @click="uppyDashboard.openModal();uppyDashboard.showPanel('Audio');"
    >
        <i aria-hidden="true" class="fa fa-microphone" />
    </div>
</template>

<script>

import * as config from '@/config.js';

export default {
    props: ['network', 'uppyDashboard'],
    computed: {
        accountOnly() {
            return config.getSetting('userAccountsOnly');
        },
        currentUser() {
            return this.network.currentUser();
        },
    },
};
</script>
