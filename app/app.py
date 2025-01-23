import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk

import plots as pl  # Custom module (e.g., for plot_confusion_matrix)

# ------------------------------------------------
# 1. Database Connection and Data Fetch Functions
# ------------------------------------------------
def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host="database",
        port=5432,
        database="fraud",
        user="docker",
        password="docker"
    )

def fetch_data(query):
    """
    Execute the provided SQL query against the connected database
    and return the result as a pandas DataFrame.
    """
    conn = connect_to_db()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Connect at script start (optional; some prefer lazy connections)
connect_to_db()

# ---------------------------
# 2. Fetch Core Table Data
# ---------------------------

transactions = fetch_data("SELECT * FROM predictions")
customers    = fetch_data("SELECT * FROM customer")

# We only need these columns for the map
locations = transactions[[
    "shipment_location_lat",
    "shipment_location_long",
    "city",
    "is_fraud"
]]

# Separate the data for fraud vs. non-fraud
locations_fraud = locations[locations["is_fraud"] == 1]
locations_clean = locations[locations["is_fraud"] == 0]

# -----------------------------
# 3. Streamlit Page Configuration
# -----------------------------
st.title("PostgreSQL Data Visualization")
st.header("Fraud Detection Analysis")
st.subheader("Insights on Purchase Behavior")


st.write(
    """
    This dashboard provides insights into fraud detection by analyzing transaction patterns, 
    including model performance (via a confusion matrix), average purchase trends, 
    and geographic distribution of transactions.
    """
)

# -----------------------------
# 4. Key Metrics & Visualizations
# -----------------------------
st.markdown("### Key Metrics")
st.markdown("1. Confusion Matrix to visualize model performance")
st.markdown("2. Average purchase amount by fraud status")
st.markdown("3. Purchases per Transaction by Fraud Status")
st.markdown("4. Payment Method Fraud Rate Bubble Chart")
st.markdown("5. Geographic Distribution of Transactions")

if st.button("Refresh Data"):
    st.rerun()

# 4a. Plot Confusion Matrix
st.markdown("#### Confusion Matrix")
st.markdown("The confusion matrix below shows the model's performance in predicting fraud transactions.")
confusion_matrix = pl.plot_confusion_matrix(transactions)  # Custom function from 'plots' module
st.plotly_chart(confusion_matrix, use_container_width=True)

# 4b. Bar Chart of Average Purchases
st.markdown("#### Average Purchase Amount by Fraud Status")

# Group transactions by fraud status and compute mean of 'purchases_count'
purchases_count = transactions.groupby("is_fraud")["purchases_count"].mean().reset_index()

# Map 0 → Non-Fraud, 1 → Fraud
purchases_count["is_fraud"] = purchases_count["is_fraud"].map({0: "Non-Fraud", 1: "Fraud"})

st.write(
    """
    The bar chart below compares the average purchase amounts between non-fraudulent and
    fraudulent transactions, providing a quick view of how transaction size correlates
    with fraud status.
    """
)
st.bar_chart(
    data=purchases_count,
    x="is_fraud",
    y="purchases_count",
    x_label="Fraud Status",
    y_label="Average Purchases Amount Per Transaction",
    use_container_width=True
)


#---------------------------------------------------------------
# 4c. Histogram of Fraudulent Transactions
#---------------------------------------------------------------
st.markdown("#### Purchases per Transaction by Fraud Status")
fraud_hist = pl.plot_fraud_histogram(transactions)  # Custom function from 'plots' module
st.plotly_chart(fraud_hist, use_container_width=True)
st.write("The histogram above shows the distribution of purchases per transaction for fraud and non-fraud cases. As we can see, "
         "a larger proportion of fraud transactions involve a higher number of purchases per transaction, which could be a potential indicator of fraudulent activity.")
#---------------------------------------------------------------
# 4d. Bubble Chart
#---------------------------------------------------------------
st.markdown("#### Payment Method Fraud Rate Bubble Chart")
bubble_chart = pl.plot_fraud_bubble_chart(transactions)  # Custom function from 'plots' module
st.plotly_chart(bubble_chart, use_container_width=True)
st.write("The bubble chart above visualizes the fraud rate and transaction volume for each payment method. The size of each bubble represents the number of fraud transactions, "
            "while the color intensity indicates the fraud rate. This chart can help identify payment methods with higher fraud rates and transaction volumes.")

# ---------------------------------------
# 5. Interactive Map of Transaction Data
# ---------------------------------------
st.markdown("### Geographic Distribution of Transactions")
st.write(
    """
    Below is an interactive map visualizing fraud (red) and non-fraud (blue) transactions.
    Use the checkboxes to include or exclude transaction types.
    """
)

# 5a. Checkboxes to filter data
fraud_checkbox = st.checkbox("Fraud Transactions", value=True)
non_fraud_checkbox = st.checkbox("Non-Fraud Transactions", value=True)

# Combine selected data into a single DataFrame
df_to_show = pd.DataFrame()
if fraud_checkbox:
    df_to_show = pd.concat([df_to_show, locations_fraud])
if non_fraud_checkbox:
    df_to_show = pd.concat([df_to_show, locations_clean])

# If no data selected, display warning
if df_to_show.empty:
    st.warning("No data selected. Please check at least one box above.")
    st.stop()

# 5b. Create a 'color' column indicating red for fraud, blue for non-fraud
def get_color(is_fraud):
    return [255, 0, 0] if is_fraud == 1 else [0, 0, 255]

df_to_show["color"] = df_to_show["is_fraud"].apply(get_color)

# 5c. Define the scatterplot layer using pydeck
mean_lat = df_to_show["shipment_location_lat"].mean()
mean_lon = df_to_show["shipment_location_long"].mean()

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df_to_show,
    get_position=["shipment_location_long", "shipment_location_lat"],
    get_fill_color="color",
    get_radius=5000,  # Adjust radius to fit your data scale
    pickable=True     # Allows for tooltips on hover
)

# 5d. Initial view state (camera position)
view_state = pdk.ViewState(
    latitude=mean_lat if not pd.isna(mean_lat) else 0,
    longitude=mean_lon if not pd.isna(mean_lon) else 0,
    zoom=2,
    pitch=0
)

# 5e. Render the pydeck map
r = pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",  # or other styles (dark-v9, etc.)
    initial_view_state=view_state,
    layers=[layer],
    tooltip={
        "html": "<b>City:</b> {city}<br/><b>Fraud:</b> {is_fraud}",
        "style": {"color": "white"}
    }
)

st.pydeck_chart(r)

st.write(
    """
    Each dot on the map represents a transaction.
    **Red** indicates a fraudulent transaction, and **blue** indicates a legitimate one.  Overall, fraudulent  and
    non-fraudulent transactions share a similar geographic distribution. However, the map can help identify any
    potential clustering or patterns in fraudulent transactions such as hotspots in certain cities or regions.
    """
)

