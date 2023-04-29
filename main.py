from flask import Flask, render_template, send_file, request, redirect, url_for, flash
import os
from flask_cors import CORS, cross_origin
import shutil
from prediction_Validation_Insertion import pred_validation
from predictFromModel import predictFromModelSingle, predictFromModelBulk


app=Flask(__name__)
CORS(app)
app.secret_key = "any random string"


@app.route("/", methods=['GET'])
@cross_origin()
def index():
    return render_template('index.html')

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html")


@app.route("/predict", methods=['POST'])
@cross_origin()
def predict():
    try:
        Item_Identifier=request.form['Item_Identifier']
        Item_Weight=float(request.form['Item_Weight'])
        Item_Fat_Content=request.form['Item_Fat_Content']
        Item_Visibility=float(request.form['Item_Visibility'])
        Item_Type=request.form['Item_Type']
        Item_MRP=float(request.form['Item_MRP'])
        Outlet_Identifier=request.form['Outlet_Identifier']
        Outlet_Establishment_Year=int(request.form['Outlet_Establishment_Year'])
        Outlet_Size=request.form['Outlet_Size']
        Outlet_Location_Type=request.form['Outlet_Location_Type']
        Outlet_Type=request.form['Outlet_Type']


        data={
            "Item_Identifier" : Item_Identifier,
            "Item_Weight" : Item_Weight,
            "Item_Fat_Content" : Item_Fat_Content,
            "Item_Visibility" : Item_Visibility,
            "Item_Type" : Item_Type,
            "Item_MRP" : Item_MRP,
            "Outlet_Identifier" : Outlet_Identifier,
            "Outlet_Establishment_Year" : Outlet_Establishment_Year,
            "Outlet_Size" : Outlet_Size,
            "Outlet_Location_Type" : Outlet_Location_Type,
            "Outlet_Type" : Outlet_Type,

        }

        pred = predictFromModelSingle.prediction(data)
        output = pred.predictionFromModel()
        flash(f"The predicted Item Outlet Sales : {output}", "success")

        return redirect(url_for('index'))

    except Exception as e:
        flash('Something went wrong', 'danger')
        return redirect(url_for('index'))
        # raise e


@app.route("/predict-dataset", methods=['POST'])
@cross_origin()
def predict_dataset():
    try:
        files = request.files.getlist('files')

        folderName = 'Prediction_Batch_Files'
        if  os.path.isdir(folderName):
            shutil.rmtree(folderName)
        os.mkdir(folderName)

        for file in files:
            file.save(os.path.join(folderName , file.filename))

        pred_val = pred_validation(folderName)  # object initialization
        path=pred_val.prediction_validation()  # calling the prediction_validation function


        pred = predictFromModelBulk.prediction(path)  # object initialization
        # predicting for dataset present in database
        output_folder = pred.predictionFromModel()

        return send_file(output_folder, as_attachment=True)

    except Exception as e:
        flash('Something went wrong', 'danger')
        return redirect(url_for('index'))
        # raise e




if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
