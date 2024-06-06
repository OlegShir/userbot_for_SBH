import asyncio, os, enum
from datetime import datetime, timedelta
from pyrogram import Client
from sqlalchemy import Column, Integer, DateTime, Enum, select, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Определяем базовый класс для моделей
Base = declarative_base()

# Статусы пользователя
class UserStatus(enum.Enum):
    alive = "alive"
    dead = "dead"
    finished = "finished"

# Определяем модель пользователя в PG
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(Enum(UserStatus), default=UserStatus.alive)
    status_updated_at = Column(DateTime, default=datetime.utcnow)

# Конфигурация базы данных
engine = create_async_engine(os.environ.get('DATABASE_URL'), echo=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# Настройка Pyrogram
app = Client("User_bot", os.environ.get('API_ID'), os.environ.get('API_HASH'))

# Сообщения и время отправки
messages = [
    {"text": "Текст1", "delay": timedelta(minutes=6)},
    {"text": "Текст2", "delay": timedelta(minutes=39), "trigger": "Триггер1"},
    {"text": "Текст3", "delay": timedelta(days=1, hours=2)}
]

# Проверка наличия триггерных слов
async def check_triggers(client: Client, user_id: int) -> bool:
    async for message in client.get_chat_history(user_id, limit=100):
        if any(word in message.text.lower() for word in ["прекрасно", "ожидать"]):
            return True
    return False

# Функция для отправки сообщений
async def send_message(client: Client, user: User, message:dict) -> bool:
    try:
        await client.send_message(user.id, message["text"])
        return True
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю {user.id}: {e}")
        return False

# Основной цикл
async def main() -> None:
    while True:
        async with SessionLocal() as session:
            # Получаем пользователей со статусом alive
            result = await session.execute(select(User).where(User.status == UserStatus.alive))
            users = result.scalars().all()
            
            now = datetime.utcnow()

            # Проверка и отправка сообщений
            for user in users:
                for i, message in enumerate(messages):
                    send_time = user.created_at + sum(m["delay"] for m in messages[:i + 1])
                    if now >= send_time:
                        if "trigger" in message or await check_triggers(app, user.id):
                            await session.execute(update(User).where(User.id == user.id).values(status=UserStatus.finished, status_updated_at=now))
                            break

                        if await send_message(app, user, message):
                            user.created_at = now  # Обновляем время отправки последнего сообщения
                        else:
                            await session.execute(update(User).where(User.id == user.id).values(status=UserStatus.dead, status_updated_at=now))
                            break

            await session.commit()
        await asyncio.sleep(60)  # Пауза между проверками

if __name__ == "__main__":
    asyncio.run(main())