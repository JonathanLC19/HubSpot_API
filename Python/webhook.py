import base64
import hashlib
import hmac
from flask import Flask, request, abort

app = Flask(__name__)

# Reemplaza esto por tu client secret de la Private App
CLIENT_SECRET = 'tu-client-secret-aqui'

def verify_signature(request):
    hubspot_signature = request.headers.get('X-Hubspot-Signature')
    
    if not hubspot_signature:
        return False

    # Usamos el body en bytes en lugar de convertir a texto
    request_body = request.data
    print(f"Body de la solicitud: {request_body}")

    # Generamos el hash HMAC con el client secret
    hash_hmac = hmac.new(
        key=bytes(CLIENT_SECRET, 'utf-8'),
        msg=request_body,  # Usamos los datos en crudo
        digestmod=hashlib.sha256
    ).digest()

    # Convertimos el hash a formato Base64
    hash_hmac_base64 = base64.b64encode(hash_hmac).decode('utf-8')

    print(f"Firma generada: {hash_hmac_base64}")
    print(f"Firma recibida: {hubspot_signature}")

    # Comparamos la firma generada con la firma enviada por HubSpot
    return hmac.compare_digest(hash_hmac_base64, hubspot_signature)

@app.route('/webhook', methods=['POST'])
def webhook():
    if not verify_signature(request):
        # Si la firma es inválida, respondemos con 403
        abort(403)

    # Si la firma es válida, procesamos el webhook
    data = request.json
    print('Payload recibido:', data)
    return 'Webhook recibido', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)