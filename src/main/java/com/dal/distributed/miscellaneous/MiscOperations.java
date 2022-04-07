package com.dal.distributed.miscellaneous;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.utils.FileOperations;

import java.io.IOException;

public class MiscOperations {
    public static void createInitFolders() throws IOException {
        FileOperations.createNewFolderRecursively(AuthConstants.AUTHENTICATION_FOLDER);
        FileOperations.createNewFolderRecursively(DataConstants.LOGS_FILE_LOCATION);
        FileOperations.createNewFolderRecursively(DataConstants.DATABASES_FOLDER_LOCATION);
    }
}
