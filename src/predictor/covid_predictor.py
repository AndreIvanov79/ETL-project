import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import os

def train_model(df: pd.DataFrame, model_path='covid_model.pkl'):
    if df.empty:
        print("There is no data to train. The model will not be trained..")
        return

    X = df[['tavg', 'prcp', 'pres']]
    y = df['cases']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    print(f"R² score on test: {score:.3f}")

    joblib.dump(model, model_path)
    print(f"Model saved: {model_path}")

def predict_cases(model_path, tavg, prcp, pressure):
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}. Train the model first.")

    model = joblib.load(model_path)
    prediction = model.predict([[tavg, prcp, pressure]])
    return round(prediction[0])


# Optional training script
# if __name__ == '__main__':
#     from src.db.db_manager import DBManager
#     db = DBManager()
#     df = db.get_covid_weather_for_training()
#     train_model(df)

