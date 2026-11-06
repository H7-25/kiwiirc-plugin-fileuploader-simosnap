<template>
    <div
        class="p-fileuploader-upload-button"
        @click="openUploadDashboard('Audio')"
    >
        <i aria-hidden="true" class="fa fa-microphone" />
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
