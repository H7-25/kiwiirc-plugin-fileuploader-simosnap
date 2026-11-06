export function showDashboardOnDragEnter(openUploadDashboard) {
    return function handleDragEnter(event) {
        const types = Array.from(event.dataTransfer?.types || []);

        if (!types.includes('Files')) {
            return;
        }

        openUploadDashboard();
    };
}
