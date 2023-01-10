#
# Autogenerated by Thrift Compiler (0.14.1)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import iotdb.thrift.common.ttypes

from thrift.transport import TTransport
all_structs = []


class TCreateTrainingTaskReq(object):
    """
    Attributes:
     - modelId
     - isAuto
     - modelConfigs
     - queryExpressions
     - queryFilter

    """


    def __init__(self, modelId=None, isAuto=None, modelConfigs=None, queryExpressions=None, queryFilter=None,):
        self.modelId = modelId
        self.isAuto = isAuto
        self.modelConfigs = modelConfigs
        self.queryExpressions = queryExpressions
        self.queryFilter = queryFilter

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.modelId = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.BOOL:
                    self.isAuto = iprot.readBool()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.MAP:
                    self.modelConfigs = {}
                    (_ktype1, _vtype2, _size0) = iprot.readMapBegin()
                    for _i4 in range(_size0):
                        _key5 = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                        _val6 = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                        self.modelConfigs[_key5] = _val6
                    iprot.readMapEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.LIST:
                    self.queryExpressions = []
                    (_etype10, _size7) = iprot.readListBegin()
                    for _i11 in range(_size7):
                        _elem12 = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                        self.queryExpressions.append(_elem12)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.STRING:
                    self.queryFilter = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('TCreateTrainingTaskReq')
        if self.modelId is not None:
            oprot.writeFieldBegin('modelId', TType.STRING, 1)
            oprot.writeString(self.modelId.encode('utf-8') if sys.version_info[0] == 2 else self.modelId)
            oprot.writeFieldEnd()
        if self.isAuto is not None:
            oprot.writeFieldBegin('isAuto', TType.BOOL, 3)
            oprot.writeBool(self.isAuto)
            oprot.writeFieldEnd()
        if self.modelConfigs is not None:
            oprot.writeFieldBegin('modelConfigs', TType.MAP, 4)
            oprot.writeMapBegin(TType.STRING, TType.STRING, len(self.modelConfigs))
            for kiter13, viter14 in self.modelConfigs.items():
                oprot.writeString(kiter13.encode('utf-8') if sys.version_info[0] == 2 else kiter13)
                oprot.writeString(viter14.encode('utf-8') if sys.version_info[0] == 2 else viter14)
            oprot.writeMapEnd()
            oprot.writeFieldEnd()
        if self.queryExpressions is not None:
            oprot.writeFieldBegin('queryExpressions', TType.LIST, 5)
            oprot.writeListBegin(TType.STRING, len(self.queryExpressions))
            for iter15 in self.queryExpressions:
                oprot.writeString(iter15.encode('utf-8') if sys.version_info[0] == 2 else iter15)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.queryFilter is not None:
            oprot.writeFieldBegin('queryFilter', TType.STRING, 6)
            oprot.writeString(self.queryFilter.encode('utf-8') if sys.version_info[0] == 2 else self.queryFilter)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.modelId is None:
            raise TProtocolException(message='Required field modelId is unset!')
        if self.isAuto is None:
            raise TProtocolException(message='Required field isAuto is unset!')
        if self.modelConfigs is None:
            raise TProtocolException(message='Required field modelConfigs is unset!')
        if self.queryExpressions is None:
            raise TProtocolException(message='Required field queryExpressions is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class TDeleteModelReq(object):
    """
    Attributes:
     - modelPath

    """


    def __init__(self, modelPath=None,):
        self.modelPath = modelPath

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.modelPath = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('TDeleteModelReq')
        if self.modelPath is not None:
            oprot.writeFieldBegin('modelPath', TType.STRING, 1)
            oprot.writeString(self.modelPath.encode('utf-8') if sys.version_info[0] == 2 else self.modelPath)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.modelPath is None:
            raise TProtocolException(message='Required field modelPath is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class TForecastReq(object):
    """
    Attributes:
     - modelPath
     - dataset

    """


    def __init__(self, modelPath=None, dataset=None,):
        self.modelPath = modelPath
        self.dataset = dataset

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.modelPath = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.dataset = []
                    (_etype19, _size16) = iprot.readListBegin()
                    for _i20 in range(_size16):
                        _elem21 = iprot.readBinary()
                        self.dataset.append(_elem21)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('TForecastReq')
        if self.modelPath is not None:
            oprot.writeFieldBegin('modelPath', TType.STRING, 1)
            oprot.writeString(self.modelPath.encode('utf-8') if sys.version_info[0] == 2 else self.modelPath)
            oprot.writeFieldEnd()
        if self.dataset is not None:
            oprot.writeFieldBegin('dataset', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.dataset))
            for iter22 in self.dataset:
                oprot.writeBinary(iter22)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.modelPath is None:
            raise TProtocolException(message='Required field modelPath is unset!')
        if self.dataset is None:
            raise TProtocolException(message='Required field dataset is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class TForecastResp(object):
    """
    Attributes:
     - status
     - forecastResult

    """


    def __init__(self, status=None, forecastResult=None,):
        self.status = status
        self.forecastResult = forecastResult

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = iotdb.thrift.common.ttypes.TSStatus()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.forecastResult = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('TForecastResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.forecastResult is not None:
            oprot.writeFieldBegin('forecastResult', TType.STRING, 2)
            oprot.writeBinary(self.forecastResult)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        if self.forecastResult is None:
            raise TProtocolException(message='Required field forecastResult is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(TCreateTrainingTaskReq)
TCreateTrainingTaskReq.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'modelId', 'UTF8', None, ),  # 1
    None,  # 2
    (3, TType.BOOL, 'isAuto', None, None, ),  # 3
    (4, TType.MAP, 'modelConfigs', (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), None, ),  # 4
    (5, TType.LIST, 'queryExpressions', (TType.STRING, 'UTF8', False), None, ),  # 5
    (6, TType.STRING, 'queryFilter', 'UTF8', None, ),  # 6
)
all_structs.append(TDeleteModelReq)
TDeleteModelReq.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'modelPath', 'UTF8', None, ),  # 1
)
all_structs.append(TForecastReq)
TForecastReq.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'modelPath', 'UTF8', None, ),  # 1
    (2, TType.LIST, 'dataset', (TType.STRING, 'BINARY', False), None, ),  # 2
)
all_structs.append(TForecastResp)
TForecastResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [iotdb.thrift.common.ttypes.TSStatus, None], None, ),  # 1
    (2, TType.STRING, 'forecastResult', 'BINARY', None, ),  # 2
)
fix_spec(all_structs)
del all_structs
