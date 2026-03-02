import dask.dataframe as dd
import matplotlib.pyplot as plt

# Load dataset using Dask
df = dd.read_csv("online_retail.csv",
                 dtype={'InvoiceNo': 'str'})

print("Dataset Loaded Successfully")

# Remove null values
df = df.dropna()

# Create TotalPrice column
df["TotalPrice"] = df["Quantity"] * df["UnitPrice"]

# ==========================
# 1️⃣ Total Revenue
# ==========================
total_revenue = df["TotalPrice"].sum().compute()
print("Total Revenue:", total_revenue)

# ==========================
# 2️⃣ Top Countries
# ==========================
country_sales = df.groupby("Country")["TotalPrice"].sum().compute().sort_values(ascending=False)

print("\nTop Countries by Revenue:")
print(country_sales.head(10))

# ==========================
# 3️⃣ Top Products
# ==========================
top_products = df.groupby("Description")["Quantity"].sum().compute().sort_values(ascending=False)

print("\nTop Products:")
print(top_products.head(10))

# ==========================
# 4️⃣ Monthly Trend
# ==========================
df["InvoiceDate"] = dd.to_datetime(df["InvoiceDate"], errors="coerce")
df["Month"] = df["InvoiceDate"].dt.month

monthly_sales = df.groupby("Month")["TotalPrice"].sum().compute().sort_index()

print("\nMonthly Revenue:")
print(monthly_sales)

# ==========================
# Visualization
# ==========================
country_sales.head(5).plot(kind="bar")
plt.title("Top 5 Countries by Revenue")
plt.xlabel("Country")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.show()