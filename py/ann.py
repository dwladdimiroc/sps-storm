import numpy as np

from sklearn.neural_network import MLPRegressor


def ann_prediction(samples, prediction_number):
    t = list(range(len(samples)))
    # print(t)
    # print(samples)
    mlp = MLPRegressor(random_state=1, max_iter=500).fit(np.array(t).reshape(-1, 1), samples)

    predictions = []
    for x in range(prediction_number):
        predictions.append(mlp.predict(np.array([len(samples) + x]).reshape(-1, 1))[0])
        # print(f"predicted f({len(samples) + x}): {mlp.predict(np.array([len(samples) + x]).reshape(-1, 1))}")

    next_input = np.average(predictions)
    return next_input, predictions
