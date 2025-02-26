package org.apache.iotdb.db.pipe.connector.protocol;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.WString;
import com.sun.jna.platform.win32.COM.IUnknown;
import com.sun.jna.platform.win32.COM.Unknown;
import com.sun.jna.platform.win32.Guid;
import com.sun.jna.platform.win32.Guid.IID;
import com.sun.jna.platform.win32.Ole32;
import com.sun.jna.platform.win32.OleAuto;
import com.sun.jna.platform.win32.Variant;
import com.sun.jna.platform.win32.WTypes;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinError;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.ptr.ShortByReference;
import org.jinterop.dcom.core.JIVariant;

import java.util.Arrays;
import java.util.List;

public class OpcDaCreateGroupDemo {

  // OPC DA Server 的 CLSID（替换为实际值，例如 Matrikon 模拟服务器的 CLSID）
  private static final Guid.CLSID CLSID_OPC_SERVER =
      new Guid.CLSID("CAE8D0E1-117B-11D5-924B-11C0F023E91C");

  // IOPCServer 接口的 IID（固定值，来自 OPC DA 规范）
  private static final Guid.IID IID_IOPCServer = new IID("39C13A4D-011E-11D0-9675-0020AFD8ADB3");

  // IOPCItemMgt 接口的 IID
  private static final IID IID_IOPCItemMgt = new IID("39C13A54-011E-11D0-9675-0020AFD8ADB3");
  // IOPCSyncIO 接口的 IID
  private static final IID IID_IOPCSyncIO = new IID("39C13A52-011E-11D0-9675-0020AFD8ADB3");

  // IUnknown 的 IID（固定值）
  private static final Guid.IID IID_IUNKNOWN = new IID("00000000-0000-0000-C000-000000000046");

  public static void main(String[] args) {
    // 初始化 COM 库（多线程模式）
    Ole32.INSTANCE.CoInitializeEx(null, Ole32.COINIT_MULTITHREADED);

    PointerByReference ppvServer = new PointerByReference();
    try {
      // 1. 创建 OPC DA Server 实例
      WinNT.HRESULT hr =
          Ole32.INSTANCE.CoCreateInstance(
              CLSID_OPC_SERVER,
              null,
              0x17,
              IID_IOPCServer, // 直接请求 IOPCServer 接口
              ppvServer);

      if (hr.intValue() != WinError.S_OK.intValue()) {
        System.err.println("连接失败，错误码: 0x" + Integer.toHexString(hr.intValue()));
        return;
      }

      // 2. 获取 IOPCServer 接口
      IOPCServer opcServer = new IOPCServer(ppvServer.getValue());

      // 3. 创建 Group
      PointerByReference phServerGroup = new PointerByReference();
      PointerByReference phOPCGroup = new PointerByReference();
      IntByReference pRevisedUpdateRate = new IntByReference();
      int hr2 =
          opcServer.AddGroup(
              "", // 组名（空字符串表示自动生成）
              true, // 组是否激活（TRUE）
              1000, // 请求的更新速率（毫秒）
              0, // 客户端组句柄（可设为0）
              null, // 时区偏移（通常为null）
              null, // 死区百分比（通常为null）
              0, // 区域ID（通常为0）
              phServerGroup, // 返回的服务端组句柄
              pRevisedUpdateRate, // 返回的实际更新速率
              new Guid.GUID.ByReference(IID_IUNKNOWN.getPointer()), // 请求的接口类型（IUnknown）
              phOPCGroup // 返回的组接口指针
              );

      if (hr2 == WinError.S_OK.intValue()) {
        System.out.println("成功创建 Group！");
        System.out.println("服务端组句柄: " + phServerGroup.getValue());
        System.out.println("实际更新速率: " + pRevisedUpdateRate.getValue() + " ms");
      } else {
        System.err.println("创建 Group 失败，错误码: 0x" + Integer.toHexString(hr.intValue()));
      }

      IUnknown groupUnknown = new Unknown(phOPCGroup.getValue());
      // 3. 获取 IOPCItemMgt 接口（用于添加 Item）
      PointerByReference ppvItemMgt = new PointerByReference();
      hr =
          groupUnknown.QueryInterface(
              new Guid.REFIID(new Guid.GUID.ByReference(IID_IOPCItemMgt).getPointer()), ppvItemMgt);
      if (hr.intValue() == WinError.S_OK.intValue()) {
        System.out.println("获取 IOPCItemMgt 成功！");
        System.out.println("服务端组句柄: " + ppvItemMgt.getValue());
      } else {
        System.err.println("获取 IOPCItemMgt 失败，错误码: 0x" + Integer.toHexString(hr.intValue()));
        return;
      }

      IOPCItemMgt itemMgt = new IOPCItemMgt(ppvItemMgt.getValue());

      // 4. 添加 Item（例如写入的 Tag 名称）
      String itemId = "StringValue"; // 替换为实际 Item ID
      OPCITEMDEF[] itemDefs = new OPCITEMDEF[1];
      itemDefs[0] = new OPCITEMDEF();
      itemDefs[0].szAccessPath = new WString("");
      itemDefs[0].szItemID = new WString(itemId + "\0");
      itemDefs[0].bActive = 1;
      itemDefs[0].hClient = 0;
      itemDefs[0].dwBlobSize = 0;
      itemDefs[0].pBlob = Pointer.NULL;
      itemDefs[0].vtRequestedDataType = Variant.VT_BSTR;
      itemDefs[0].wReserved = 0;
      itemDefs[0].write();

      PointerByReference ppItemResults = new PointerByReference();
      PointerByReference ppErrors = new PointerByReference();
      hr2 = itemMgt.AddItems(1, itemDefs, ppItemResults, ppErrors);

      if (hr2 == WinError.S_OK.intValue()) {
        System.out.println("添加 Item 成功！");
      } else {
        Pointer pErrors = ppErrors.getValue();
        if (pErrors != null) {
          // 读取错误码数组，每个错误码对应一个 Item
          int[] errors = pErrors.getIntArray(0, 1); // 这里添加了1个Item，所以读取1个元素
          int itemError = errors[0];

          if (itemError == WinError.S_OK.intValue()) {
            System.out.println("添加 Item 成功！");
          } else {
            System.err.println("Item 错误码: 0x" + Integer.toHexString(itemError));
            return;
          }
          Ole32.INSTANCE.CoTaskMemFree(pErrors);
        }
        System.err.println("添加 Item 失败，错误码: 0x" + Integer.toHexString(hr2));
        return;
      }

      Pointer pItemResults = ppItemResults.getValue();

      OPCITEMRESULT[] itemResults = new OPCITEMRESULT[1];
      itemResults[0] = new OPCITEMRESULT(pItemResults);
      itemResults[0].read();

      int serverHandle = itemResults[0].hServer; // Server 端句柄

      // 5. 获取 IOPCSyncIO 接口
      PointerByReference ppvSyncIO = new PointerByReference();
      hr =
          groupUnknown.QueryInterface(
              new Guid.REFIID(new Guid.GUID.ByReference(IID_IOPCSyncIO).getPointer()), ppvSyncIO);
      if (hr.intValue() == WinError.S_OK.intValue()) {
        System.out.println("获取 IOPCSyncIO 成功！");
        System.out.println("服务端组句柄: " + ppvSyncIO.getValue());
      } else {
        System.err.println("获取 IOPCSyncIO 失败，错误码: 0x" + Integer.toHexString(hr.intValue()));
        return;
      }
      IOPCSyncIO syncIO = new IOPCSyncIO(ppvSyncIO.getValue());

      Variant.VARIANT value = new Variant.VARIANT();

      WTypes.BSTR bstr = OleAuto.INSTANCE.SysAllocString("FuckYourMotherTwice");
      value.setValue(Variant.VT_BSTR, bstr);

      // value.setValue(Variant.VT_I4, new WinDef.LONG(0));

      value.write();

      // 7. 同步写入
      Pointer phServer = new Memory(Native.getNativeSize(int.class));
      phServer.write(0, new int[] {serverHandle}, 0, 1);

      ppErrors = new PointerByReference();
      hr2 = syncIO.Write(1, phServer, value.getPointer(), ppErrors);

      if (hr2 == WinError.S_OK.intValue()) {
        System.out.println("写入成功！");
      } else {
        Pointer pErrors = ppErrors.getValue();
        if (pErrors != null) {
          // 读取错误码数组，每个错误码对应一个 Item
          int[] errors = pErrors.getIntArray(0, 1); // 这里写入了1个数据，所以读取1个元素
          int itemError = errors[0];

          if (itemError == WinError.S_OK.intValue()) {
            System.out.println("写入成功！");
          } else {
            System.err.println("写入错误码: 0x" + Integer.toHexString(itemError));
            return;
          }
          Ole32.INSTANCE.CoTaskMemFree(pErrors);
        }
        System.err.println("写入失败，错误码: 0x" + Integer.toHexString(hr2));
      }

      // 8. 释放资源
      OleAuto.INSTANCE.SysFreeString(bstr);
      syncIO.Release();
      itemMgt.Release();
      opcServer.Release();
    } finally {
      // 释放 COM 对象
      if (ppvServer.getValue() != null) {
        Ole32.INSTANCE.CoTaskMemFree(ppvServer.getValue());
      }
      // 卸载 COM 库
      Ole32.INSTANCE.CoUninitialize();
    }
  }

  // 定义 IOPCServer 接口（部分方法）
  public static class IOPCServer extends Unknown {
    public IOPCServer(Pointer p) {
      super(p);
    }

    // AddGroup 方法在 vtable 中的索引为 3（前3个是IUnknown方法）
    public int AddGroup(
        String szName, // [in] LPCWSTR
        boolean bActive, // [in] BOOL
        int dwRequestedUpdateRate, // [in] DWORD
        int hClientGroup, // [in] OPCHANDLE
        Pointer pTimeBias, // [in] LONG*（通常为null）
        Pointer pPercentDeadband, // [in] FLOAT*（通常为null）
        int dwLCID, // [in] DWORD
        PointerByReference phServerGroup, // [out] OPCHANDLE*
        IntByReference pRevisedUpdateRate, // [out] DWORD*
        Guid.GUID.ByReference riid, // [in] REFIID
        PointerByReference ppUnk // [out] IUnknown**
        ) {
      // 将 Java 字符串转换为 COM 的宽字符串
      WString wName = new WString(szName);

      return this._invokeNativeInt(
          3,
          new Object[] {
            this.getPointer(),
            wName,
            bActive ? 1 : 0,
            dwRequestedUpdateRate,
            hClientGroup,
            pTimeBias,
            pPercentDeadband,
            dwLCID,
            phServerGroup,
            pRevisedUpdateRate,
            riid != null ? riid.getPointer() : null,
            ppUnk
          });
    }
  }

  // IOPCItemMgt 接口定义（部分）
  // DWORD dwCount,
  // OPCITEMDEF *pItemArray,
  // OPCITEMRESULT **ppAddResults,
  // HRESULT **ppErrors) = 0;
  public static class IOPCItemMgt extends Unknown {
    public IOPCItemMgt(Pointer p) {
      super(p);
    }

    public int AddItems(
        int dwCount,
        OPCITEMDEF[] pItemArray,
        PointerByReference pResults,
        PointerByReference pErrors) {
      return this._invokeNativeInt(
          3, new Object[] {this.getPointer(), dwCount, pItemArray, pResults, pErrors});
    }
  }

  // DWORD dwCount,
  // OPCHANDLE  *phServer,
  // VARIANT *pItemValues,
  // HRESULT **ppErrors
  // IOPCSyncIO 接口定义（部分）
  public static class IOPCSyncIO extends Unknown {
    public IOPCSyncIO(Pointer p) {
      super(p);
    }

    public int Write(
        int dwCount, Pointer phServer, Pointer pItemValues, PointerByReference pErrors) {
      return this._invokeNativeInt(
          4,
          new Object[] { // Write 是第4个方法
            this.getPointer(), dwCount, phServer, pItemValues, pErrors
          });
    }
  }

  public static class OPCITEMDEF extends Structure {
    public WString szAccessPath = new WString(""); // 访问路径（通常为空字符串）
    public WString szItemID; // 数据项 ID（如 "Channel1.Device1.Tag1"）
    public int bActive; // 是否激活（TRUE=1, FALSE=0）
    public int hClient; // 客户端句柄
    public int dwBlobSize; // BLOB 数据大小
    public Pointer pBlob; // BLOB 数据指针
    public short vtRequestedDataType = JIVariant.VT_UNKNOWN;
    public short wReserved;

    // 必须指定字段顺序，与 C 结构体一致
    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "szAccessPath",
          "szItemID",
          "bActive",
          "hClient",
          "dwBlobSize",
          "pBlob",
          "vtRequestedDataType",
          "wReserved");
    }
  }

  public static class OPCITEMRESULT extends Structure {
    public int hServer; // 服务端句柄
    public short vtCanonicalDataType; // 数据类型（如 Variant.VT_R8）
    public short wReserved; // 保留字段
    public int dwAccessRights; // 访问权限
    public int dwBlobSize; // BLOB 数据大小
    public Pointer pBlob; // BLOB 数据指针

    public OPCITEMRESULT() {
      super(ALIGN_MSVC); // 4 字节对齐
    }

    public OPCITEMRESULT(Pointer pointer) {
      super(pointer);
    }

    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "hServer", "vtCanonicalDataType", "wReserved", "dwAccessRights", "dwBlobSize", "pBlob");
    }
  }
}
