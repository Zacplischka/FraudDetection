import pandas as pd
import plotly.express as px


def plot_confusion_matrix(df, actual_col='is_fraud', predicted_col='prediction'):
    """
    Plots a confusion matrix as a heatmap using Plotly, with "Fraud" and "Non-Fraud" labels.

    Parameters:
        df (pd.DataFrame): DataFrame containing actual and predicted values.
        actual_col (str): Column name for actual values (default: 'is_fraud').
        predicted_col (str): Column name for predicted values (default: 'prediction').

    Returns:
        fig (plotly.graph_objects.Figure): The generated Plotly heatmap figure.
    """
    # Ensure original DataFrame is not modified
    df = df.copy()

    # Map fraud labels to "Non-Fraud" and "Fraud"
    df[actual_col] = df[actual_col].map({0: 'Non-Fraud', 1: 'Fraud'})
    df[predicted_col] = df[predicted_col].map({0: 'Non-Fraud', 1: 'Fraud'})

    # Create the confusion matrix
    confusion_matrix = pd.crosstab(
        df[actual_col],
        df[predicted_col],
        rownames=['Actual'],
        colnames=['Predicted']
    )

    # Create heatmap with labels
    fig = px.imshow(
        confusion_matrix,
        title="Confusion Matrix",
        labels=dict(x="Predicted", y="Actual", color="Count"),
        text_auto=True  # Show count values inside each square
    )

    return fig  # Return the figure for further use


def plot_average_purchase(df, fraud_col='is_fraud', purchase_col='purchases_count'):
    """
    Plots a bar chart showing the average purchase amount by fraud status.

    Parameters:
        df (pd.DataFrame): DataFrame containing fraud status and purchase count.
        fraud_col (str): Column name representing fraud status (default: 'is_fraud').
        purchase_col (str): Column name representing purchase counts (default: 'purchases_count').

    Returns:
        fig (plotly.graph_objects.Figure): The generated Plotly bar chart.
    """
    df = df.copy()

    # Map fraud labels to "Non-Fraud" and "Fraud"
    df[fraud_col] = df[fraud_col].map({0: 'Non-Fraud', 1: 'Fraud'})

    # Calculate average purchase amount per fraud status
    avg_purchase = df.groupby(fraud_col)[purchase_col].mean().reset_index()

    # Create bar plot with custom labels
    fig = px.bar(
        avg_purchase,
        x=fraud_col,
        y=purchase_col,
        title='Average Purchase Amount by Fraud Status',
        labels={fraud_col: 'Fraud Status', purchase_col: 'Average Purchase Amount'}
    )

    return fig


import pandas as pd
import plotly.express as px

def plot_fraud_bubble_chart(df: pd.DataFrame):
    """
    Plots a fraud rate bubble chart for payment methods with labeled bubbles.

    - X-Axis: Total transactions per payment method
    - Y-Axis: Fraud rate (%) per payment method
    - Bubble Size: Number of fraud transactions
    - Color: Fraud rate intensity
    - Labels: Display payment method names directly on the bubbles

    Parameters:
        df (pd.DataFrame): A DataFrame containing 'payment_method' and 'is_fraud' columns.

    Returns:
        plotly.graph_objects.Figure: The generated bubble chart.
    """
    df = df.copy()

    # Aggregate fraud data
    fraud_data = df.groupby("payment_method").agg(
        total_transactions=("is_fraud", "count"),
        fraud_count=("is_fraud", lambda x: (x == 1).sum()),
    ).reset_index()

    # Calculate fraud rate
    fraud_data["fraud_rate"] = (fraud_data["fraud_count"] / fraud_data["total_transactions"]) * 100

    # Create bubble chart with labels
    fig = px.scatter(
        fraud_data,
        x="total_transactions",
        y="fraud_rate",
        size="fraud_count",
        color="fraud_rate",
        text="payment_method",  # Add payment method labels directly on the bubbles
        title="Fraud Rate vs. Transaction Volume by Payment Method",
        labels={"total_transactions": "Total Transactions", "fraud_rate": "Fraud Rate (%)"},
        hover_data=["payment_method"],  # Show payment method on hover
        size_max=50,  # Adjust bubble size
        color_continuous_scale="Reds"  # Fraud rate intensity (red = higher fraud)
    )

    # Improve layout for readability
    fig.update_layout(
        xaxis_title="Total Transactions",
        yaxis_title="Fraud Rate (%)",
        coloraxis_colorbar_title="Fraud Rate (%)"
    )

    # Improve label positioning
    fig.update_traces(textposition="middle center")  # Center labels within bubbles

    return fig


def plot_fraud_histogram(df, x_col='purchases_count', fraud_col='is_fraud', log_scale=True):
    """
    Plots a histogram showing the distribution of a selected column (e.g., 'purchases_count')
    with fraud status differentiation.

    Parameters:
        df (pd.DataFrame): DataFrame containing transaction data.
        x_col (str): Column name for the x-axis (default: 'purchases_count').
        fraud_col (str): Column name indicating fraud status (default: 'is_fraud').
        log_scale (bool): Whether to apply a logarithmic scale to the y-axis (default: True).

    Returns:
        fig (plotly.graph_objects.Figure): The generated Plotly histogram.
    """
    df = df.copy()

    # Map fraud status to readable labels
    df[fraud_col] = df[fraud_col].map({0: 'Non-Fraud', 1: 'Fraud'})

    # Create histogram with custom colors and correct y-axis label
    fig = px.histogram(
        df,
        x=x_col,
        color=fraud_col,
        title=f'{x_col.replace("_", " ").title()} Distribution by Fraud Status',
        barmode='overlay',  # Overlay fraud and non-fraud
        nbins=50,  # Increase number of bins for better spread
        histnorm='percent',  # Normalize to percentage
        labels={x_col: x_col.replace("_", " ").title(), fraud_col: "Fraud Status"},
        color_discrete_map={"Fraud": "red", "Non-Fraud": "blue"}  # Ensure fraud is red, non-fraud is blue
    )

    # Explicitly set y-axis title to "Percentage"
    fig.update_layout(yaxis_title="Percentage (Log Scale)")

    # Apply logarithmic scale if enabled
    if log_scale:
        fig.update_layout(yaxis_type="log")

    return fig  # Return the figure for further use if needed

# plots.py
import plotly.express as px

def plot_locations(df):
    """
    Creates a single scatter_mapbox chart colored by 'is_fraud'.
    Fraud = red, Non-fraud = blue.
    """
    fig = px.scatter_mapbox(
        df,
        lat="shipment_location_lat",
        lon="shipment_location_long",
        hover_name="city",
        zoom=1,
        mapbox_style="open-street-map",
        color="is_fraud",
        color_discrete_map={
            0: "blue",  # Non-fraud
            1: "red"    # Fraud
        }
    )
    return fig


