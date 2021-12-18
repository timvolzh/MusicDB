import psycopg2
import sqlalchemy
from pprint import pprint
import csv


'''Данные для занесения в БД'''
singer_list = ["Стас Пьеха", "Григорий Лепс", "Slava Marlow", "Мари Краймбрери", "Dan Balan", "Niletto", "Selena Gomez",
               "Фактор 2"]
album_list = [
    {'name': "Жажда фейерверков", 'release_year': 1985},
    {'name': "Фиолетовый", 'release_year': 2000},
    {'name': "Туманы", 'release_year': 2015},
    {'name': "Перекресток небес", 'release_year': 2018},
    {'name': "Слёзы айтишников", 'release_year': 2001},
    {'name': "Вомбат", 'release_year': 2018},
    {'name': "Приоры и кальяны", 'release_year': 2020},
    {'name': "Кровавая любовь", 'release_year': 2021}]
genre_list = ["SoftDarkMetalRock", "Hard-Pop", "Cute-Country", "Yellow-Rap", "Child-Jazz"]
song_list = [
    {'name': "Мой Резус-фактор", 'duration': 2.3, 'album': 6},
    {'name': "Уважаемая", 'duration': 3.3, 'album': 4},
    {'name': "Пометка штурмана", 'duration': 4.1, 'album': 7},
    {'name': "Покачивая пропеллер", 'duration': 2.1, 'album': 5},
    {'name': "Заповедь лыжника", 'duration': 3.6, 'album': 3},
    {'name': "Агентура", 'duration': 3.5, 'album': 8},
    {'name': "Отвесный взгляд", 'duration': 2.9, 'album': 1},
    {'name': "Условие", 'duration': 3.1, 'album': 2},
    {'name': "Хранитель винокурни", 'duration': 2.4, 'album': 3},
    {'name': "Pool Starfall", 'duration': 1.8, 'album': 4},
    {'name': "Undocked Untangle", 'duration': 2.4, 'album': 1},
    {'name': "Расширение Lazy", 'duration': 2.2, 'album': 7},
    {'name': "Двухнедельный застой", 'duration': 3.2, 'album': 3},
    {'name': "Двойственный Барабан", 'duration': 2.7, 'album': 8},
    {'name': "Гибридизация", 'duration': 3.1, 'album': 4},
    {'name': "Рисовать", 'duration': 5.2, 'album': 2}
]
collection_list = [
    {'name': "Расстояние", 'release_year': 2021},
    {'name': "Красочный", 'release_year': 2018},
    {'name': "Спидвей хулиганства", 'release_year': 2019},
    {'name': "Космогония", 'release_year': 1995},
    {'name': "Ежегодный Медик", 'release_year': 2001},
    {'name': "Поверхностный эксперимент", 'release_year': 1983},
    {'name': "Линейка", 'release_year': 1998},
    {'name': "Артикуляция", 'release_year': 1999}
]
albumsinger_list = [
    {'singer': 1, 'album': 1},
    {'singer': 1, 'album': 2},
    {'singer': 2, 'album': 3},
    {'singer': 3, 'album': 4},
    {'singer': 4, 'album': 5},
    {'singer': 5, 'album': 6},
    {'singer': 6, 'album': 7},
    {'singer': 7, 'album': 8},
    {'singer': 8, 'album': 1},
    {'singer': 1, 'album': 2},
    {'singer': 2, 'album': 3},
    {'singer': 3, 'album': 4}
]
collectionsong_list = [
    {'collection':1, 'song': 2},
    {'collection':1, 'song': 3},
    {'collection':1, 'song': 4},
    {'collection':1, 'song': 5},
    {'collection':1, 'song': 6},
    {'collection':1, 'song': 7},
    {'collection':2, 'song': 8},
    {'collection':2, 'song': 9},
    {'collection':2, 'song': 10},
    {'collection':2, 'song': 11},
    {'collection':2, 'song': 12},
    {'collection':3, 'song': 13},
    {'collection':3, 'song': 14},
    {'collection':3, 'song': 15},
    {'collection':3, 'song': 1},
    {'collection':4, 'song': 2},
    {'collection':4, 'song': 3},
    {'collection':4, 'song': 4},
    {'collection':4, 'song': 5},
    {'collection':5, 'song': 6},
    {'collection':6, 'song': 7},
    {'collection':6, 'song': 8},
    {'collection':6, 'song': 9},
    {'collection':6, 'song': 10},
    {'collection':6, 'song': 11},
    {'collection':7, 'song': 12},
    {'collection':7, 'song': 13},
    {'collection':7, 'song': 14},
    {'collection':7, 'song': 15},
    {'collection':7, 'song': 1},
    {'collection':8, 'song': 2},
    {'collection':8, 'song': 3},
    {'collection':8, 'song': 4},
]
singergenre_list = [
    {'singer': 1, 'genre': 5},
    {'singer': 2, 'genre': 4},
    {'singer': 3, 'genre': 3},
    {'singer': 4, 'genre': 2},
    {'singer': 5, 'genre': 1},
    {'singer': 6, 'genre': 5},
    {'singer': 7, 'genre': 4},
    {'singer': 8, 'genre': 3}
]


# Подключение БД
db = 'postgresql://postgres:1111@localhost:5432/postgres'
engine = sqlalchemy.create_engine(db)
connection = engine.connect()


'''Создание и вызов функций, вносящих данные'''
def add_singer(singers):
    values = str()
    i = 1
    for singer in singers:
        values += f"({i}, '{singer}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO singer VALUES{values}'
    connection.execute(request)


def add_album(albums):
    values = str()
    i = 1
    for album in albums:
        values += f"({i}, '{album['name']}', '{album['release_year']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO album VALUES{values}'
    connection.execute(request)


def add_genre(genres):
    values = str()
    i = 1
    for genre in genres:
        values += f"({i}, '{genre}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO genre VALUES{values}'
    connection.execute(request)


def add_song(songs):
    values = str()
    i = 1
    for song in songs:
        values += f"({i}, '{song['name']}', '{song['duration']}', '{song['album']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO song VALUES{values}'
    connection.execute(request)


def add_collection(collections):
    values = str()
    i = 1
    for collection in collections:
        values += f"({i}, '{collection['name']}', '{collection['release_year']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO collection VALUES{values}'
    connection.execute(request)


def add_albumsinger(albumsingers):
    values = str()
    i = 1
    for albumsinger in albumsingers:
        values += f"({i}, '{albumsinger['singer']}', '{albumsinger['album']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO albumsinger VALUES{values}'
    connection.execute(request)


def add_collectionsong(collectionsongs):
    values = str()
    i = 1
    for collectionsong in collectionsongs:
        values += f"({i}, '{collectionsong['collection']}', '{collectionsong['song']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO collectionsong VALUES{values}'
    connection.execute(request)


def add_singergenre(singergenres):
    values = str()
    i = 1
    for singergenre in singergenres:
        values += f"({i}, '{singergenre['singer']}', '{singergenre['genre']}'),"
        i += 1
    values = values[:-1] + ";"
    print(values)
    request = f'INSERT INTO singergenre VALUES{values}'
    connection.execute(request)


# add_singer(singer_list)
# add_album(album_list)
# add_genre(genre_list)
# add_song(song_list)
# add_collection(collection_list)
# add_albumsinger(albumsinger_list)
# add_collectionsong(collectionsong_list)
# add_singergenre(singergenre_list)


'''Получение и запись в файл результатов запросов'''

# res1 - название и год выхода альбомов, вышедших в 2018 году
res1 = connection.execute("SELECT album_name, release_year FROM album WHERE release_year=2018;").fetchmany(10)
with open('res1.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res1)


# res2 - название и продолжительность самого длительного трека
res2 = connection.execute(f"SELECT song_name, duration FROM song where duration = (select max(duration) from song);").fetchmany(10)
with open('res2.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res2)


# res3 - название треков, продолжительность которых не менее 3,5 минуты
res3 = connection.execute("SELECT song_name FROM song WHERE duration>=3.5;").fetchmany(10)
with open('res3.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res3)


# res4 - названия сборников, вышедших в период с 2018 по 2020 год включительно
res4 = connection.execute("SELECT collection_name FROM collection WHERE release_year BETWEEN 2018 AND 2020;").fetchmany(10)
with open('res4.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res4)


# res5 - исполнители, чье имя состоит из 1 слова
res5 = connection.execute("SELECT singer_name FROM singer WHERE singer_name NOT LIKE '%% %%';").fetchmany(10)
with open('res5.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res5)


# res6 - название треков, которые содержат слово "мой"/"my"
res6 = connection.execute("SELECT song_name FROM song WHERE song_name iLIKE '%%мой%%' OR song_name iLIKE '%%my%%';").fetchmany(10)
with open('res6.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res6)

