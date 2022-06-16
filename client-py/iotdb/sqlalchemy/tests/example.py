from sqlalchemy import create_engine, Column, Float, BigInteger, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

metadata = MetaData(
    schema='root.factory'
)
Base = declarative_base(metadata=metadata)


class Device(Base):
    __tablename__ = "room2.device1"
    Time = Column(BigInteger, primary_key=True)
    temperature = Column(Float)
    status = Column(Float)


engine = create_engine("iotdb://root:root@127.0.0.1:6667")
# 创建session
DbSession = sessionmaker(bind=engine)
session = DbSession()

res = session.query(Device.status, Device.Time).filter(Device.temperature > 1)

for row in res:
    print(row)
