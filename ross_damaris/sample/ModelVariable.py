# automatically generated by the FlatBuffers compiler, do not modify

# namespace: sample

import flatbuffers

class ModelVariable(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsModelVariable(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ModelVariable()
        x.Init(buf, n + offset)
        return x

    # ModelVariable
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ModelVariable
    def VarName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # ModelVariable
    def VarValueType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # ModelVariable
    def VarValue(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            from flatbuffers.table import Table
            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

def ModelVariableStart(builder): builder.StartObject(3)
def ModelVariableAddVarName(builder, varName): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(varName), 0)
def ModelVariableAddVarValueType(builder, varValueType): builder.PrependUint8Slot(1, varValueType, 0)
def ModelVariableAddVarValue(builder, varValue): builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(varValue), 0)
def ModelVariableEnd(builder): return builder.EndObject()
