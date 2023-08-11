import requests
import json
from datetime import datetime
from google.cloud import storage



def obtener_resenas_estado(estados_ciudades, google_api_key, yelp_api_key):
    reviews_data = []
    
    for estado, ciudades in estados_ciudades.items():
        for ciudad in ciudades:
            # Obtener datos de Google Places
            query = f"restaurant+in+{ciudad}+{estado}"
            url = f'https://maps.googleapis.com/maps/api/place/textsearch/json?key={google_api_key}&query={query}'
            response = requests.get(url)
            data = response.json()
            
            if 'results' in data and len(data['results']) > 0:
                place_ids = [result['place_id'] for result in data['results']]
                
                for place_id in place_ids:
                    url = f'https://maps.googleapis.com/maps/api/place/details/json?key={google_api_key}&place_id={place_id}&fields=name,formatted_address,reviews,types,price_level,user_ratings_total,geometry/location'
                    response = requests.get(url)
                    data = response.json()
                    
                    if 'result' in data:
                        restaurant_name = data['result'].get('name')
                        restaurant_location = data['result'].get('formatted_address')
                        reviews = data['result'].get('reviews', [])
                        category = data['result'].get('types', [])
                        price_level = data['result'].get('price_level')
                        user_ratings_total = data['result'].get('user_ratings_total')
                        location = data['result'].get('geometry', {}).get('location')
                        
                        for review in reviews:
                            review['restaurant_name'] = restaurant_name
                            review['restaurant_location'] = restaurant_location
                            review['city'] = ciudad
                            review['gmap_id'] = place_id
                            review['category'] = category
                            review['price_level'] = price_level
                            review['user_ratings_total'] = user_ratings_total
                            review['latitude'] = location.get('lat') if location else None
                            review['longitude'] = location.get('lng') if location else None
                            review['state'] = estado
                            review['source'] = 'Google'
                            
                            reviews_data.append(review)

            # Obtener datos de Yelp
            headers = {'Authorization': f'Bearer {yelp_api_key}'}
            params = {'term': 'restaurant', 'location': f'{ciudad}, {estado}'}
            url = 'https://api.yelp.com/v3/businesses/search'
            response = requests.get(url, headers=headers, params=params)
            data = response.json()

            if 'businesses' in data and len(data['businesses']) > 0:
                for business in data['businesses']:
                    url = f'https://api.yelp.com/v3/businesses/{business["id"]}/reviews'
                    response = requests.get(url, headers=headers)
                    data = response.json()
                    
                    if 'reviews' in data:
                        for review in data['reviews']:
                            review['restaurant_name'] = business['name']
                            review['restaurant_location'] = business['location']['address1']
                            review['city'] = ciudad
                            review['gmap_id'] = business['id']
                            review['category'] = business['categories']
                            review['price_level'] = None
                            review['user_ratings_total'] = None
                            review['latitude'] = business['coordinates']['latitude']
                            review['longitude'] = business['coordinates']['longitude']
                            review['state'] = estado
                            review['source'] = 'Yelp'
                            
                            reviews_data.append(review)
    
    return reviews_data

# clave API de Google y Yelp
google_api_key = "clave-api"
yelp_api_key = "clave-api"
estados_ciudades = {
    'New York': ['Ciudad de Nueva York', 'Niagara Falls', 'Buffalo', 'Rochester', 'Albany', 'Syracuse', 'Long Island', 'Ithaca', 'Montauk', 'Cooperstown'],
    'California': ['Los Ángeles', 'San Francisco', 'San Diego', 'Napa Valley', 'Yosemite National Park', 'Lake Tahoe', 'Santa Barbara', 'Palm Springs', 'Big Sur', 'Hollywood'],
    'Florida': ['Miami', 'Orlando', 'Tampa', 'Fort Lauderdale', 'Miami Beach', 'Key West', 'Everglades National Park', 'St. Augustine', 'Sarasota', 'Fort Myers']
}
reviews_data = obtener_resenas_estado(estados_ciudades, google_api_key, yelp_api_key)

#Almacenamiento en el bucket
if reviews_data:
    filename = f"reviews_gm_all_.json"
    client = storage.Client()
    bucket = client.bucket('nombre del bucket')
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(reviews_data))

    print(f"Datos de reseñas guardados en Google Cloud Storage: {filename}")
else:
    print("No se encontraron reseñas en ningún estado")