from fastapi import FastAPI, HTTPException
from typing import List
from . import models
from .database import get_conn, _close_conn

app = FastAPI(title="Pizza API")

def _restaurant_exists(restaurant_id: int) -> bool:
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM restaurants WHERE id=%s", (restaurant_id,))
        return bool(c.fetchone())
    finally:
        _close_conn(conn)

def _fetch_pizza_ingredients(c, pizza_id: int) -> List[str]:
    c.execute("""
        SELECT i.name
        FROM pizza_ingredients pi
        JOIN ingredients i ON pi.ingredient_id = i.id
        WHERE pi.pizza_id = %s
    """, (pizza_id,))
    return [row["name"] for row in c.fetchall()]

@app.get("/restaurants", response_model=List[models.Restaurant])
def get_restaurants():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id, name, address FROM restaurants")
        return c.fetchall()
    finally:
        _close_conn(conn)

@app.post("/restaurants", response_model=models.Restaurant)
def add_restaurant(restaurant: models.Restaurant):
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            "INSERT INTO restaurants (name, address) VALUES (%s, %s) RETURNING id",
            (restaurant.name, restaurant.address)
        )
        restaurant.id = c.fetchone()["id"]
        conn.commit()
        return restaurant
    finally:
        _close_conn(conn)

@app.get("/restaurants/{restaurant_id}/menu", response_model=List[models.Pizza])
def get_restaurant_menu(restaurant_id: int):
    if not _restaurant_exists(restaurant_id):
        raise HTTPException(status_code=404, detail="Ресторан не найден")
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT id, name, cheese, dough, secret_ingredient, restaurant_id
            FROM pizzas WHERE restaurant_id=%s
        """, (restaurant_id,))
        pizzas = []
        for row in c.fetchall():
            row_dict = dict(row)
            row_dict["ingredients"] = _fetch_pizza_ingredients(c, row_dict["id"])
            pizzas.append(row_dict)
        return pizzas
    finally:
        _close_conn(conn)

@app.get("/pizzas", response_model=List[models.Pizza])
def get_pizzas():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id, name, cheese, dough, secret_ingredient, restaurant_id FROM pizzas")
        pizzas = []
        for row in c.fetchall():
            row_dict = dict(row)
            row_dict["ingredients"] = _fetch_pizza_ingredients(c, row_dict["id"])
            pizzas.append(row_dict)
        return pizzas
    finally:
        _close_conn(conn)

@app.post("/pizzas", response_model=models.Pizza)
def add_pizza(pizza: models.Pizza):
    if not _restaurant_exists(pizza.restaurant_id):
        raise HTTPException(status_code=400, detail="Ресторан не найден")
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO pizzas (name, cheese, dough, secret_ingredient, restaurant_id)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (pizza.name, pizza.cheese, pizza.dough, pizza.secret_ingredient, pizza.restaurant_id))
        pizza.id = c.fetchone()["id"]
        for ing_name in pizza.ingredients:
            c.execute("SELECT id FROM ingredients WHERE name=%s", (ing_name,))
            ing_row = c.fetchone()
            if ing_row:
                c.execute(
                    "INSERT INTO pizza_ingredients (pizza_id, ingredient_id) VALUES (%s, %s)",
                    (pizza.id, ing_row["id"])
                )
        conn.commit()
        return pizza
    finally:
        _close_conn(conn)

@app.put("/pizzas/{pizza_id}", response_model=models.Pizza)
def update_pizza(pizza_id: int, upd: models.Pizza):
    if not _restaurant_exists(upd.restaurant_id):
        raise HTTPException(status_code=400, detail="Ресторан не найден")
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM pizzas WHERE id=%s", (pizza_id,))
        if not c.fetchone():
            raise HTTPException(status_code=404, detail="Пицца не найдена")
        c.execute("""
            UPDATE pizzas
            SET name=%s, cheese=%s, dough=%s, secret_ingredient=%s, restaurant_id=%s
            WHERE id=%s
        """, (upd.name, upd.cheese, upd.dough, upd.secret_ingredient, upd.restaurant_id, pizza_id))
        c.execute("DELETE FROM pizza_ingredients WHERE pizza_id=%s", (pizza_id,))
        for ing_name in upd.ingredients:
            c.execute("SELECT id FROM ingredients WHERE name=%s", (ing_name,))
            ing_row = c.fetchone()
            if ing_row:
                c.execute(
                    "INSERT INTO pizza_ingredients (pizza_id, ingredient_id) VALUES (%s, %s)",
                    (pizza_id, ing_row["id"])
                )
        conn.commit()
        upd.id = pizza_id
        return upd
    finally:
        _close_conn(conn)

@app.delete("/pizzas/{pizza_id}", status_code=204)
def delete_pizza(pizza_id: int):
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id FROM pizzas WHERE id=%s", (pizza_id,))
        if not c.fetchone():
            raise HTTPException(status_code=404, detail="Пицца не найдена")
        c.execute("DELETE FROM pizza_ingredients WHERE pizza_id=%s", (pizza_id,))
        c.execute("DELETE FROM pizzas WHERE id=%s", (pizza_id,))
        conn.commit()
    finally:
        _close_conn(conn)

@app.get("/ingredients", response_model=List[models.Ingredient])
def get_ingredients():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id, name FROM ingredients")
        return c.fetchall()
    finally:
        _close_conn(conn)

@app.get("/chefs", response_model=List[models.Chef])
def get_chefs():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("SELECT id, name, restaurant_id FROM chefs")
        return c.fetchall()
    finally:
        _close_conn(conn)

@app.post("/chefs", response_model=models.Chef)
def add_chef(chef: models.Chef):
    if not _restaurant_exists(chef.restaurant_id):
        raise HTTPException(status_code=400, detail="Ресторан не найден")
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            "INSERT INTO chefs (name, restaurant_id) VALUES (%s, %s) RETURNING id",
            (chef.name, chef.restaurant_id)
        )
        chef.id = c.fetchone()["id"]
        conn.commit()
        return chef
    finally:
        _close_conn(conn)

@app.get("/reviews", response_model=List[models.ReviewOut])
def get_reviews():
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT r.id, r.restaurant_id, r.rating, r.text, t.name AS restaurant_name
            FROM reviews r
            JOIN restaurants t ON r.restaurant_id = t.id
        """)
        reviews = []
        for r in c.fetchall():
            reviews.append(models.ReviewOut(
                id=r["id"],
                restaurant_name=r["restaurant_name"],
                rating=r["rating"],
                text=r["text"]
            ))
        return reviews
    finally:
        _close_conn(conn)

@app.post("/reviews", response_model=models.Review)
def add_review(review: models.Review):
    if not _restaurant_exists(review.restaurant_id):
        raise HTTPException(status_code=400, detail="Ресторан не найден")
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            "INSERT INTO reviews (restaurant_id, rating, text) VALUES (%s, %s, %s) RETURNING id",
            (review.restaurant_id, review.rating, review.text)
        )
        review.id = c.fetchone()["id"]
        conn.commit()
        return review
    finally:
        _close_conn(conn)

        Al hasano de dursuno ф