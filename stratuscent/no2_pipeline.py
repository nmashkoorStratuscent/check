import kfp.dsl as dsl
import kfp.components as comp
from kubernetes import client as k8s
import kfp.gcp as gcp
from kfp import components
from string import Template
import yaml
import json

@dsl.pipeline(
    name='NO2 Pipeline',
    description='End to end NO2 Pipeline to predict the NO2 analytes'
)

def no2_pipeline(
    postive_data_file='gcr.io/<$PROJECT_ID>/no2/step1_loadingdata:v1',
    output_positive_data_file='gcr.io/<$PROJECT_ID>/no2/step2_analytes:v1',
    analyte_name='no2',
    run_name='no2',
    plot_dir = 'gs://<$BUCKET_NAME>/no2/plots',
    start_from=0,
    max_len=10000,
    div_by=800,
    before_after=100,
    norm_bias_factor=-1,
    sensormoduleid_or_deviceid='sensor_module_id',
    batch_size=16,
    epochs=200,
    layers=32,
    learning_rate=5e-3,
    serving_namespace='default',
    serving_name='no2-serving',
    image='gcr.io/<$PROJECT_ID>/no2/step3_serving:v1',
):

    #PVC: Persistent Volume Claim
    vop = comp.VolumeOp(
        name='my-pvc',
        resource_name = 'no2-pvc',
        size='30Gi',
        modes = dsl.VOLUME_MODE_RWO
    )

    positive_data_load = comp.ContainerOp(
        name='positive_data_load',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/positive_data_load.py',
            '--input_dir','/data/positive',
        ])

    negative_data_load = comp.ContainerOp(
        name='negative_data_load',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/negative_data_load.py',
            '--input_dir','/data/negative',
        ])
    
    merge_pos_neg_data = comp.ContainerOp(
        name='merge_pos_neg_data',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/merge_pos_neg_data.py',
            '--output_file','/data/positive_negative_data.csv',
        ],
        pvolumes = {'/meged_data': vop.volume}
    ).apply(gcp.use_gcp_secret('user-gcp-sa').after(positive_data_load, negative_data_load))

    process_data = comp.ContainerOp(
        name='process_data',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/process_data.py',
            '--input_file','/data/positive_negative_data.csv',
            '--output_file','/data/processed_data.csv',
            '--analyte_name',analyte_name,
            '--run_name',run_name,
            '--plot_dir',plot_dir,
            '--start_from',start_from,
            '--max_len',max_len,
            '--div_by',div_by,
            '--before_after',before_after,
            '--norm_bias_factor',norm_bias_factor,
            '--sensormoduleid_or_deviceid',sensormoduleid_or_deviceid,
        ], 
        pvolumes = {'/processed_data': vop.volume}
    ).apply(gcp.use_gcp_secret('user-gcp-sa').after(merge_pos_neg_data))

    pad_data = comp.ContainerOp(
        name='pad_data',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/pad_data.py',
            '--input_file','/data/processed_data.csv',
            '--output_file','/data/padded_data.csv',
            '--batch_size',batch_size,
        ],
        pvolumes = {'/padded_data': vop.volume},
        outputs = ['/data/padded_data.csv']
    ).apply(gcp.use_gcp_secret('user-gcp-sa').after(process_data))


    train_model = comp.ContainerOp(
        name='train_model',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/train_model.py',
            '--input_file','/data/padded_data.csv',
            '--output_file','/data/model.h5',
            '--epochs',epochs,
            '--layers',layers,
            '--learning_rate',learning_rate,
        ],
        pvolumes = {'/model': vop.volume},
        outputs = ['/data/model.h5']
    ).apply(gcp.use_gcp_secret('user-gcp-sa').after(pad_data))

    evaluate_model = comp.ContainerOp(
        name='evaluate_model',
        image='gcr.io/stratuscent-public/no2-pipeline:latest',
        command='python3',
        arguments=[
            '/app/evaluate_model.py',
            '--input_file','/data/padded_data.csv',
            '--model_file','/data/model.h5',
            '--analyte_name',analyte_name,
            '--run_name',run_name,
            '--plot_dir',plot_dir,
            '--start_from',start_from,
            '--max_len',max_len,
            '--div_by',div_by,
            '--before_after',before_after,
            '--norm_bias_factor',norm_bias_factor,
            '--sensormoduleid_or_deviceid',sensormoduleid_or_deviceid,
        ],
        pvolumes = {'/model': vop.volume},
        outputs = ['/data/model.h5']
    ).apply(gcp.use_gcp_secret('user-gcp-sa').after(train_model))

    kfserving_template = Template("""{
                              "apiVersion": "serving.kubeflow.org/v1alpha2",
                              "kind": "InferenceService",
                              "metadata": {
                                "labels": {
                                  "controller-tools.k8s.io": "1.0"
                                },
                                "name": "$name",
                                "namespace": "$namespace"
                              },
                              "spec": {
                                "default": {
                                  "predictor": {
                                    "custom": {
                                      "container": {
                                        "image": "$image"
                                      }
                                    }
                                  }
                                }
                              }
                            }""")
    kfservingjson = kfserving_template.substitute({ 'name': str(serving_name),
                                'namespace': str(serving_namespace),
                                'image': str(image)})

    
    kfservingdeployment = json.loads(kfservingjson)

    serve = dsl.ResourceOp(
        name='serve',
        k8s_resource=kfservingdeployment,
        action= 'apply',
        success_condition='status.url',
    )
    serve.apply(gcp.use_gcp_secret('user-gcp-sa').after(evaluate_model))


if __name__ == '__main__':
    import kfp.compiler as compiler
    pipeline_func = no2_pipeline
    pipeline_filename = pipeline_func.__name__ + '.pipeline.zip'
    compiler.Compiler().compile(pipeline_func, pipeline_filename)


    
