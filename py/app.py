from flask import Flask, request, jsonify

from ann import ann_prediction
from fft import fft_prediction
from random_forest import rf_prediction
from regression_linear import regression_linear_prediction

app = Flask(__name__)


@app.route('/')
def test():  # put application's code here
    return 'Test'


@app.route('/random_forest', methods=["POST"])
def random_forest():
    samples = request.json['samples']
    prediction_number = request.json['prediction_number']
    avg_prediction, predictions = rf_prediction(samples, prediction_number)
    resp = {
        "avg_prediction": avg_prediction,
        "predictions": predictions,
    }
    return jsonify(resp)


@app.route('/ann', methods=["POST"])
def ann():
    samples = request.json['samples']
    prediction_number = request.json['prediction_number']
    avg_prediction, predictions = ann_prediction(samples, prediction_number)
    resp = {
        "avg_prediction": avg_prediction,
        "predictions": predictions,
    }
    return jsonify(resp)


@app.route('/fft', methods=["POST"])
def fft():
    samples = request.json['samples']
    prediction_number = request.json['prediction_number']
    avg_prediction, predictions = fft_prediction(samples, prediction_number)
    resp = {
        "avg_prediction": avg_prediction,
        "predictions": predictions,
    }
    return jsonify(resp)


@app.route('/regression_linear', methods=["POST"])
def regression_linear():
    samples = request.json['samples']
    prediction_number = request.json['prediction_number']
    avg_prediction, predictions = regression_linear_prediction(samples, prediction_number)
    resp = {
        "avg_prediction": avg_prediction,
        "predictions": predictions,
    }
    return jsonify(resp)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888)
