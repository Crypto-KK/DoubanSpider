from sqlalchemy import Column, String, create_engine, Integer, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://root:998219@127.0.0.1:3306/douban?charset=utf8')
Base = declarative_base()


class Comments(Base):

    __tablename__ = 'test'

    id = Column(Integer, primary_key=True)
    username = Column(String(64), nullable=False, index=True)
    user_center = Column(String(64), nullable=True)
    vote = Column(Integer, nullable=True)
    star = Column(String(10), nullable=True)
    time = Column(DateTime, nullable=True)
    content = Column(Text(), nullable=False, index=True)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.content)


def create_session():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

if __name__ == '__main__':
    Base.metadata.create_all(engine)