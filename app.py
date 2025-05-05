from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib
import pandas as pd
import streamlit as st
import numpy as np

# # --- Probability Extractor Classes (no changes) ---
# class ProbaExtractor(BaseEstimator, TransformerMixin):
#     def __init__(self, model):
#         self.model = model

#     def fit(self, X, y=None):
#         return self

#     def transform(self, X):
#         probabilities = self.model.predict_proba(X)
#         return probabilities[:, 1].reshape(-1, 1)

# class MultiClassProbaExtractor(BaseEstimator, TransformerMixin):
#     def __init__(self, model):
#         self.model = model

#     def fit(self, X, y=None):
#         return self

#     def transform(self, X):
#         probabilities = self.model.predict_proba(X)
#         return probabilities

# --- Load the model pipeline ---
model = joblib.load("combined_pipeline_final.joblib")

# --- Feature List Used in the Model ---
original_features = [
   'founded_at', 'investment_rounds', 'funding_rounds', 'funding_total_usd',
    'country_AUS', 'country_CAN', 'country_DEU', 'country_ESP', 'country_FRA',
    'country_GBR', 'country_IND', 'country_ISR', 'country_NLD', 'country_Other',
    'country_USA',
    'category_Other', 'category_advertising', 'category_biotech', 'category_consulting',
    'category_ecommerce', 'category_enterprise', 'category_games_video', 'category_mobile',
    'category_other', 'category_software', 'category_web'
]

# --- Select options ---
category_options = [
    'advertising', 'biotech', 'consulting', 'ecommerce', 'enterprise',
    'games_video', 'mobile', 'software', 'web', 'Other'
]

country_options = [
    'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'IND', 'ISR', 'NLD', 'USA', 'Other'
]

# --- Streamlit App Layout ---
st.title("🚀 Startup Status Predictor")

# --- Input fields ---
st.subheader("📥 Startup Information")
founded_year = st.number_input("Founded Year", min_value=1900, max_value=2025, step=1)
investment_rounds = st.number_input("Number of Investment Rounds", min_value=0)
funding_rounds = st.number_input("Number of Funding Rounds", min_value=0)
funding_total_usd = st.number_input("Total Funding Amount (USD)", min_value=0.0)
# milestones = st.number_input("Number of Milestones Achieved", min_value=0)
# relationships = st.number_input("Number of Relationships", min_value=0)
# lat = st.number_input("Latitude", min_value=-90.0, max_value=90.0)
# lng = st.number_input("Longitude", min_value=-180.0, max_value=180.0)
category = st.selectbox("Category of Business", category_options)
country = st.selectbox("Country", country_options)

# --- Predict Button ---
if st.button("Predict Status"):
    try:
        # Initialize all features to 0
        input_dict = {feature: 0 for feature in original_features}

        # Fill numeric values
        input_dict['founded_at'] = founded_year
        input_dict['investment_rounds'] = investment_rounds
        input_dict['funding_rounds'] = funding_rounds
        input_dict['funding_total_usd'] = funding_total_usd
        # input_dict['milestones'] = milestones
        # input_dict['relationships'] = relationships
        # input_dict['lat'] = lat
        # input_dict['lng'] = lng

        # One-hot encode country
        country_feature = f"country_{country}"
        if country_feature in input_dict:
            input_dict[country_feature] = 1
        else:
            input_dict['country_Other'] = 1

        # One-hot encode category
        category_feature = f"category_{category}"
        if category_feature in input_dict:
            input_dict[category_feature] = 1
        else:
            input_dict['category_Other'] = 1

        # Convert to DataFrame
        input_df = pd.DataFrame([input_dict])

        # DEBUGGING STEP (optional but recommended)
        st.write("🔍 Input Features to Model:", input_df)

        # Predict probabilities
        probabilities = model.predict_proba(input_df)[0]
        class_labels = model.classes_

        # Map class indices to readable names
        status_map = {
            0: "Closed",
            1: "Operating",
            2: "Acquired",
            3: "IPO"
        }

        # Find predicted class
        predicted_class = model.predict(input_df)[0]
        predicted_label = status_map.get(predicted_class, "Unknown")

        # Display prediction
        st.success(f"✅ Predicted Status: **{predicted_label.upper()}**")

        # Show probabilities nicely
        st.markdown("### 📊 Probability Distribution:")
        for i, cls in enumerate(class_labels):
            label = status_map.get(cls, f"Unknown-{cls}")
            st.write(f"{label}: {probabilities[i]:.4f}")

    except Exception as e:
        st.error(f"❌ Something went wrong: {e}")





# # --- Input fields (ab inka bhi koi khaas use nahi hai prediction logic mein) ---
# founded_year = st.number_input("Founded Year", min_value=1900, max_value=2025, step=1)
# investment_rounds = st.number_input("Number of Investment Rounds", min_value=0)
# funding_total_usd = st.number_input("Total Funding Amount (USD)", min_value=0.0)
# funding_rounds = st.number_input("Number of Funding Rounds", min_value=0)
# category_code = st.selectbox("Category of Business", ['advertising'])
# country_code = st.selectbox("Country", ['USA'])

# # --- Predict button ---
# if st.button("Predict Status "):
#     try:
#         # **HARCODED PREDICTION - REAL MODEL IS NOT BEING USED**
#         hardcoded_prediction = "ACQUIRED"  # Aap chahe toh "CLOSED" bhi hardcode kar sakte hain
#         st.success(f"✅ Predicted Status : **{hardcoded_prediction}**")

#     except Exception as e:
#         st.error(f"❌ Something went wrong : {e}")