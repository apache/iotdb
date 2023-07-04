import struct

num = 25.957603
print(num)
values_tobe_packed = []
values_tobe_packed.append(num)
value_bytes = struct.pack(">f", *values_tobe_packed)

num_new = struct.unpack(">f", value_bytes)[0]
print(num_new)