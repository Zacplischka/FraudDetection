# **Fraud Detection with PySpark**

## **Overview**
This project is a **fraud detection system** built using **Apache Spark**, **PySpark**, and **Machine Learning** techniques. It processes transactional and browsing behavior data, extracts meaningful features, and predicts fraudulent transactions. The system is designed for **scalability and efficiency**, making it suitable for large-scale fraud detection applications.

## **Features**
- **Data Ingestion**: Reads multiple CSV datasets into Spark DataFrames.
- **Feature Engineering**:
  - Event-level counts and ratios
  - Customer age, account age, and promo usage
  - Time-based features (session times, time-of-day classification)
  - Geolocation-based city extraction using `geopy`
- **Fraud Detection Model**: Uses machine learning to classify transactions as fraud or non-fraud.
- **Prediction & Output**: Saves predictions in **Parquet format** for efficient storage and retrieval.

---

## **Project Structure**
```
FraudDetection/
│── data/
│   ├── input/  # Raw datasets
│   ├── output/ # Processed data & predictions
│
│── src/
│   ├── data_processing/  # Data ingestion & preprocessing
│   ├── model_training/   # ML model training & prediction
│   ├── dashboard/        # Streamlit dashboard for visualization
│
│── notebooks/   # Jupyter Notebooks for EDA & development
│── main.py      # Main script to execute the pipeline
│── requirements.txt  # Dependencies
│── README.md    # Project documentation
```

---

## **Installation & Setup**
### **1. Clone the Repository**
```sh
git clone https://github.com/yourusername/FraudDetection.git
cd FraudDetection
```

### **2. Create a Virtual Environment**
```sh
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# On Windows: .venv\Scripts\activate
```

### **3. Install Dependencies**
```sh
pip install -r requirements.txt
```

### **4. Start a Spark Session**
Ensure that **Apache Spark** is installed and set up correctly.
```sh
pyspark --version  # Verify Spark installation
```

If you are running on a local machine, you can start a **Jupyter Notebook** to explore the data:
```sh
jupyter notebook
```

---

## **Usage**
### **1. Preprocess Data**
```sh
python main.py
```
This script:
- Loads and processes the data
- Extracts features
- Saves processed data

### **2. Train Model (Optional)**
If the model does not exist, it will be trained automatically. Otherwise, it skips training.

### **3. Make Predictions**
Predictions are saved in `data/output/predictions.parquet`.

### **4. Visualize with Streamlit Dashboard**
```sh
streamlit run src/dashboard/app.py
```
This opens an **interactive dashboard** displaying fraud detection insights.

---

## **Key Components**
### **Data Processing**
- `load_data()`: Loads CSV data into Spark DataFrames.
- `add_event_level_features()`: Computes event-based feature counts.
- `calculate_session_times()`: Extracts time-related session features.
- `enrich_feature_data()`: Merges session and customer data.
- `add_fraud_labels()`: Labels transactions as fraud/non-fraud.
- `add_city_column_spark()`: Extracts city names from latitude and longitude using `geopy`.

### **Model Training**
- `train_model()`: Trains a machine learning model for fraud detection.
- `make_predictions()`: Predicts fraud probability for new transactions.

### **Visualization**
- `plot_confusion_matrix()`: Generates a confusion matrix.
- `plot_fraud_histogram()`: Displays purchase count distribution.
- `plot_fraud_rate_by_payment()`: Shows fraud rates across payment methods.

---

## **Technical Stack**
- **Big Data Processing**: Apache Spark, PySpark
- **Data Engineering**: Pandas, NumPy, Geopy
- **Machine Learning**: Scikit-Learn, MLlib
- **Visualization**: Plotly, Streamlit
- **Storage**: Parquet, CSV

---

## **Future Enhancements**
- **Optimize Spark UDFs** for better performance
- **Integrate Kafka** for real-time fraud detection
- **Implement AutoML** for better model selection
- **Deploy API** for fraud detection as a service

---

## **Contributing**
1. **Fork** the repository.
2. **Create a new branch** (`git checkout -b feature-branch`).
3. **Make your changes** and commit (`git commit -m 'Added new feature'`).
4. **Push to your branch** (`git push origin feature-branch`).
5. **Open a Pull Request** and submit for review.

---

## **License**
This project is licensed under the **MIT License**.

---

## **Author**
👤 **Zachary Plischka**  
📧 **your.email@example.com**  
🔗 [LinkedIn](https://linkedin.com/in/yourprofile)  
🔗 [GitHub](https://github.com/yourusername)

