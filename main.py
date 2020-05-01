from fastapi import FastAPI, HTTPException, status, Response
from pydantic import BaseModel

import sqlite3

app = FastAPI()


@app.on_event('startup')
async def startup():
    app.db_connection = sqlite3.connect('chinook.db')


@app.on_event('shutdown')
async def shutdown():
    app.db_connection.close()


@app.get('/')
async def start():
    return 'Hello'


@app.get('/tracks')
async def get_tracks_per_page(page: int = 0, per_page: int = 10):
    app.db_connection.row_factory = sqlite3.Row
    tracks = app.db_connection.execute(
        "SELECT * FROM tracks ORDER BY TrackId LIMIT :per_page OFFSET :page * :per_page",
        {'page': page, 'per_page': per_page})
    return tracks.fetchall()


@app.get('/tracks/composers/')
async def get_titles(composer_name: str):
    app.db_connection.row_factory = lambda cursor, x: x[0]
    titles = app.db_connection.execute(
        "SELECT Name FROM tracks WHERE composer = ? ORDER BY name", (composer_name,)
    ).fetchall()
    if not titles:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": "Composer not found"})
    return titles


class Album(BaseModel):
    title: str
    artist_id: int


class AlbumResp(BaseModel):
    AlbumId: int
    Title: str
    ArtistId: int


@app.post('/albums', response_model=AlbumResp)
async def add_album(response: Response, rq: Album):
    artist = app.db_connection.execute(
        "SELECT name FROM artists WHERE artistid = ?", (rq.artist_id,)
    ).fetchone()
    if not artist:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": "Artist_id not found"})

    cursor = app.db_connection.execute(
        "INSERT INTO albums (Title, ArtistId) VALUES (?, ?)", (rq.title, rq.artist_id))
    app.db_connection.commit()
    response.status_code = status.HTTP_201_CREATED

    return AlbumResp(AlbumId=cursor.lastrowid, Title=rq.title, ArtistId=rq.artist_id)


@app.get('/albums/{album_id}', response_model=AlbumResp)
async def get_album(album_id: int):
    app.db_connection.row_factory = sqlite3.Row
    album = app.db_connection.execute(
        "SELECT albumid, title, artistid FROM albums WHERE albumId = ?", (album_id,)
    ).fetchone()
    if not album:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": "Composer not found"})
    return AlbumResp(**album)


class Customer(BaseModel):
    company: str = None
    address: str = None
    city: str = None
    state: str = None
    country: str = None
    postalcode: str = None
    fax: str = None


@app.post('/customers/{customer_id}')
async def update_customer(customer_id, rq: Customer):
    select_customer_query = "SELECT * FROM customers WHERE customerid = ?"
    customer = app.db_connection.execute(select_customer_query, (customer_id,)).fetchone()
    if not customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": "Customer not found"})

    dict_fields_to_update = rq.dict(exclude_unset=True)
    if dict_fields_to_update:
        keys_to_update = ', '.join([f"{k} = ?" for k, v in dict_fields_to_update.items()])
        update_customer_query = f"UPDATE customers SET {keys_to_update} WHERE customerid = ?"
        tuple_to_query = (*list(dict_fields_to_update.values()), customer_id,)
        app.db_connection.execute(update_customer_query, tuple_to_query)
        app.db_connection.commit()

    app.db_connection.row_factory = sqlite3.Row
    customer = app.db_connection.execute(select_customer_query, (customer_id,)).fetchone()

    return customer


@app.get('/sales')
async def sales_stats(category):
    app.db_connection.row_factory = sqlite3.Row
    if category=='customers':
        sales_stats = app.db_connection.execute(
            """
                SELECT
                    c.customerid, email, phone, inv.sum
                FROM customers c 
                JOIN (
                    SELECT customerid, ROUND(sum(total), 2) as sum
                    FROM invoices
                    GROUP BY customerid) inv
                ON c.customerid = inv.customerid
                ORDER BY sum DESC, c.customerid
            """
        ).fetchall()
    elif category == 'genres':
        sales_stats = app.db_connection.execute(
            """
                SELECT genres.name, SUM(quantity) AS sum
                FROM invoice_items
                    JOIN tracks ON invoice_items.TrackId = tracks.trackid
                    JOIN genres ON tracks.genreid = genres.genreid
                GROUP BY tracks.genreid ORDER BY sum DESC, genres.name
            """
        ).fetchall()
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": f"Statistics {category} not found"})
    return sales_stats
