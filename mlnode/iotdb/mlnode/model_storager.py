import os
import json
import torch
import shutil
from pylru import lrucache


ML_MODEL_DIR = 'ml_models'
CACHESIZE=30

# TODO: Concurrency
class ModelStorager(object):
    def __init__(self, root_path='ml_models', cache_size=30):
        self.root_path = root_path
        if not os.path.exists(root_path):
            os.mkdir(root_path)
        self._loaded_model_cache = lrucache(cache_size)

    def save_model(self, model, model_config, model_id, trial_id):
        fold_path = f'{self.root_path}/mid_{model_id}/'
        if not os.path.exists(fold_path):
            os.mkdir(fold_path)
        torch.jit.save(torch.jit.script(model), 
            f'{fold_path}/tid_{trial_id}.pt', 
            _extra_files={'model_config': json.dumps(model_config)})

    def load_model(self, model_id, trial_id):
        file_path = f'{self.root_path}/mid_{model_id}/tid_{trial_id}.pt'
        if model_id in self._loaded_model_cache:
            return self._loaded_model_cache[file_path]
        else:
            if not os.path.exists(file_path):
                print('Unknown model (%s)' % file_path)
                return None
            else:
                tmp_dict = {'model_config': ''}
                jit_model = torch.jit.load(file_path, _extra_files=tmp_dict)
                model_config = json.loads(tmp_dict['model_config'])
                self._loaded_model_cache[file_path] = jit_model, model_config
                return jit_model, model_config

    def _remove_from_cache(self, key):
        if key in self._loaded_model_cache:
            del self._loaded_model_cache[key]

    def delete_trial(self, model_id, trial_id):
        file_path = f'{self.root_path}/mid_{model_id}/tid_{trial_id}.pt'
        self._remove_from_cache(file_path)
        if os.path.exists(file_path):    
            os.remove(file_path)
        return not os.path.exists(file_path)

    def delete_model(self, model_id):
        folder_path = f'{self.root_path}/mid_{model_id}/'
        for file_name in os.listdir(folder_path):
            self._remove_from_cache(f'{folder_path}/{file_name}')
        if os.path.exists(folder_path):    
            shutil.rmtree(folder_path)
        return not os.path.exists(folder_path)

    def send_model():
        pass



modelStorager = ModelStorager(root_path=ML_MODEL_DIR, cache_size=CACHESIZE)

