import numpy as np

from sklearn.ensemble import RandomForestRegressor


def rf_prediction(samples, prediction_number):
    t = list(range(len(samples)))
    # print(t)
    # print(samples)
    rand_forest = RandomForestRegressor()
    rand_forest.fit(np.array(t).reshape(-1, 1), samples)

    predictions = []
    for x in range(prediction_number):
        predictions.append(rand_forest.predict(np.array([len(samples) + x]).reshape(-1, 1))[0])
        # print(f"predicted f({len(samples) + x}): {rand_forest.predict(np.array([len(samples) + x]).reshape(-1, 1))}")

    next_input = np.average(predictions)
    return next_input, predictions
