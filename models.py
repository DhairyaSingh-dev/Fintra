"""
Database Models Module
Defines the database schema for Users and their Positions.
"""
from datetime import datetime, timezone

from database import db


class User(db.Model):
    """Represents a user in the database."""
    id = db.Column(db.Integer, primary_key=True)
    google_user_id = db.Column(db.String(120), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    name = db.Column(db.String(120), nullable=True)
    picture = db.Column(db.String(512), nullable=True)
    positions = db.relationship('Position', backref='owner', lazy=True, cascade="all, delete-orphan")

    def __repr__(self):
        return f'<User {self.email}>'

class Position(db.Model):
    """Represents a single stock position held by a user."""
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, index=True)
    quantity = db.Column(db.Float, nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    entry_date = db.Column(db.Date, nullable=False, default=lambda: datetime.now(timezone.utc).date())
    notes = db.Column(db.Text, nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

    def __repr__(self):
        return f'<Position {self.symbol} for User {self.user_id}>'