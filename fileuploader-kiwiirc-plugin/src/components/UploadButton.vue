<template>
    <div
        class="p-fileuploader-upload-button"
        @click="openUploadDashboard()"
    >
        <i aria-hidden="true" class="fa fa-upload" />
    </div>
</template>

<script>
export default {
    props: {
        openUploadDashboard: {
            type: Function,
            required: true,
        },
    },
};
</script>
