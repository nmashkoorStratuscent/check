from asyncore import read
from email.mime import application
from pyexpat import model
import pandas as pd
from cloud_wrapper import GStore
import numpy as np
import pandas as pd
from io import BytesIO

KEY = r'C:\Users\User\Desktop\Strat\ml_pipeline-hafizur\stratuscent\combined\keys\testproject8008-24541b9591e3.json'
BUCKET_NAME = 'data-store-strat'

gstore = GStore(BUCKET_NAME)


a = df = pd.DataFrame({'A': [1, 2], 'B': [0.5, 0.75]},
                  index=['a', 'b'])
a.to_csv('gs://data-store-strat/sample123.csv')
# a = pd.read_csv('gs://data-store-strat/raw_data_sample/no2_positive/IAS_2106091729_100083_e868608d.csv')
# a['name'] ='Nashit'

# gstore.upload_from_memory(a.to_csv(index=False), 'nashit/check.csv', content_type='text/csv')


# b = {'a': [1,2,3], 'b':'abc'}
# import json

# # gstore.upload_from_memory(json.dumps(b), 'nashit/check.json', content_type='application/json')

# c = json.loads(gstore.download_into_memory('nashit/check.json'))

# with open('test.npy', 'rb') as reader:
#     a = np.load(reader)

# np_bytes = BytesIO()
# np.save(np_bytes, a, allow_pickle=True)
# gstore.upload_from_memory(np_bytes.getvalue(), 'nashit/test123.npy', content_type='application/octet-stream')

# b = np.load(BytesIO(gstore.download_into_memory('nashit/test12.npy')), allow_pickle=True)
# print(b, a)



if not gstore.is_file(f'models/_2022_06_16_17_50_41/no2_model.h5'):
        raise ValueError('Evaluation failed: Model weights could not be load') 


model_time = gstore.download_into_memory(f'models/latest.txt').decode()
print(model_time)