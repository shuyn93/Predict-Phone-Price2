import pickle
import ast
from xgboost import XGBRegressor

def map_brand_to_numeric(brand):
    brand_mapping = {
        'Samsung': 1,
        'Apple': 2,
        'Xiaomi': 3,
        'Oppo': 4,
        'Realme': 5,
        'Tecno': 6,
        'ZTE': 7,
        'Vivo': 8,
        'BPhone': 9,
        'Nokia': 10,
        'Google': 11,
        'Oscal': 12, 
        'TCL': 13,
        'INOI': 14
    }
    return brand_mapping.get(brand, 0)


def map_numeric_to_brand(number):
    numeric_mapping = {
        1: 'Samsung',
        2: 'Apple',
        3: 'Xiaomi',
        4: 'Oppo',
        5: 'Realme',
        6: 'Tecno',
        7: 'ZTE',
        8: 'Vivo',
        9: 'BPhone',
        10: 'Nokia',
        11: 'Google',
        12: 'Oscal', 
        13: 'TCL',
        14: 'INOI'
    }
    return numeric_mapping.get(number, 'Unknown')

def transformation(original_list):
    model = pickle.load(open('/home/hadoop/Predict-Phone-Price/ML/best_model.pkl', 'rb'))
    
    print(original_list)
    original_list = ast.literal_eval(original_list)
    
    print(original_list)
    
    new_list = [
        int(original_list[1]), #Brand name
        float(original_list[2]), #Bo nho trong
        float(original_list[3]),
        float(original_list[5]),
        float(original_list[6]),
        float(original_list[7]),
        float(original_list[8]),
        float(original_list[9])
    ]
    
    print(new_list)
    
    price = model.predict([new_list])
    
    new_list[0] = map_numeric_to_brand(float(new_list[0]))
    
    print(new_list)
    
    new_list.extend(price)
    return new_list    

# original_list = "[2,2, 128.0, 6.0, 'Apple A15 Bionic',6.1, 3279.0, 12, 12, 0, 25990000.0]"

# print(transformation(original_list))
