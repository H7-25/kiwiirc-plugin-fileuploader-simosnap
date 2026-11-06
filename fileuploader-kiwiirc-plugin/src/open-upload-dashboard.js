import * as config from '@/config';
import { getValidUploadTarget } from '@/utils/get-valid-upload-target';

export function isUploadAccountAllowed(buffer) {
    if (!config.getSetting('userAccountsOnly')) {
        return true;
    }

    return Boolean(buffer?.getNetwork()?.currentUser()?.account);
}

export function createUploadDashboardOpener({
    kiwiApi,
    dashboard,
    showAccountRequired,
}) {
    return function openUploadDashboard(panel = null) {
        let buffer;

        try {
            buffer = getValidUploadTarget(kiwiApi);
        } catch {
            return false;
        }

        if (!isUploadAccountAllowed(buffer)) {
            showAccountRequired();
            return false;
        }

        dashboard.openModal();

        if (panel) {
            dashboard.showPanel(panel);
        }

        return true;
    };
}
