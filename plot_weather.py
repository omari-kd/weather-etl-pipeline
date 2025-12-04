import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to SQLite
conn = sqlite3.connect("weather.db")

# Choose city to plot
city = "London"

# Load data
df = pd.read_sql_query(f"""
    SELECT timestamp, temp 
    FROM weather
    WHERE city = '{city}'
    ORDER BY timestamp;
""", conn)

conn.close()

# Convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Plot
plt.figure(figsize=(10,5))
plt.plot(df["timestamp"], df["temp"], marker="o", linestyle="-", color="b")
plt.title(f"Temperature Trend in {city}")
plt.xlabel("Time")
plt.ylabel("Temperature (°C)")
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()
