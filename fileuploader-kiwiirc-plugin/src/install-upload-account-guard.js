import { isUploadAccountAllowed } from './open-upload-dashboard';

export function installUploadAccountGuard({
    kiwiApi,
    uppy,
    showAccountRequired,
}) {
    const beforeFileAdded = uppy.opts.onBeforeFileAdded;
    const beforeUpload = uppy.opts.onBeforeUpload;

    function canUploadFiles(files) {
        return Object.values(files).every((file) => {
            if (file.progress?.uploadComplete) {
                return true;
            }

            return isUploadAccountAllowed(
                file.kiwiFileUploaderTargetBuffer
            );
        });
    }

    uppy.setOptions({
        onBeforeFileAdded(file, files) {
            const buffer = kiwiApi.state.getActiveBuffer();

            if (!isUploadAccountAllowed(buffer)) {
                showAccountRequired();
                return false;
            }

            file.kiwiFileUploaderTargetBuffer = buffer;

            return beforeFileAdded.call(uppy.opts, file, files);
        },

        onBeforeUpload(files) {
            if (!canUploadFiles(files)) {
                showAccountRequired();
                return false;
            }

            const result = beforeUpload.call(uppy.opts, files);

            if (
                result &&
                typeof result === 'object' &&
                !canUploadFiles(result)
            ) {
                showAccountRequired();
                return false;
            }

            return result;
        },
    });
}
