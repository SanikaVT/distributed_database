package com.dal.distributed.miscellaneous;

import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.utils.FileOperations;

import java.io.IOException;

public class MiscOperations {
    public static void createInitFolders() throws IOException {
        FileOperations.createNewFolderRecursively(MiscConstants.initFolder);
        FileOperations.createNewFolderRecursively(MiscConstants.initFolder2);
    }
}
