package org.apache.iotdb.tool.common;

public enum ImportTsFileOperation {
  NONE,
  MV,
  HARDLINK,
  CP,
  DELETE,
  ;

  public static boolean isValidOperation(String operation) {
    return "none".equalsIgnoreCase(operation)
        || "mv".equalsIgnoreCase(operation)
        || "cp".equalsIgnoreCase(operation)
        || "delete".equalsIgnoreCase(operation);
  }

  public static ImportTsFileOperation getOperation(String operation, boolean isFileStoreEquals) {
    switch (operation.toLowerCase()) {
      case "none":
        return ImportTsFileOperation.NONE;
      case "mv":
        return ImportTsFileOperation.MV;
      case "cp":
        if (isFileStoreEquals) {
          return ImportTsFileOperation.HARDLINK;
        } else {
          return ImportTsFileOperation.CP;
        }
      case "delete":
        return ImportTsFileOperation.DELETE;
      default:
        // ioTPrinter.println("Args error: os/of must be one of none, mv, cp, delete");
        System.exit(Constants.CODE_ERROR);
        return null;
    }
  }
}
