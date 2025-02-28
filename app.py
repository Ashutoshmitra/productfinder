from flask import Flask, request, send_file, jsonify, Response
import os
import traceback
from werkzeug.utils import secure_filename
import tempfile
from product_finder import ProductFinder
import queue
import threading
import json
import gc
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Ensure these paths work in both local and deployed environments
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.config['CREDENTIALS_PATH'] = os.path.join(BASE_DIR, 'angular-geode-449014-n7-a2b342f63acb.json')
app.config['SPREADSHEET_ID'] = '1XnfPmeupAFauU-ZWGYigtoKduDLLMFECSKkV-pm_4jc'
app.config['SHEET_NAME'] = 'products_export_1'

# Performance optimization settings
app.config['MAX_CONCURRENT_REQUESTS'] = 10  # Number of concurrent image processing requests
app.config['CACHE_TTL'] = 3600  # Cache time-to-live in seconds (1 hour)
app.config['BATCH_SIZE'] = 50  # Number of images to process in each batch

# Store queues in a dictionary with request IDs as keys
progress_queues = {}

# Allowed file extensions
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def cleanup_old_queues():
    while True:
        now = datetime.now()
        to_delete = []
        for request_id, queue_data in progress_queues.items():
            if now - queue_data['timestamp'] > timedelta(minutes=5):
                to_delete.append(request_id)
        for request_id in to_delete:
            del progress_queues[request_id]
        threading.Event().wait(300)  # Wait 5 minutes before next cleanup

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_queues, daemon=True)
cleanup_thread.start()

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/init-progress')
def init_progress():
    try:
        request_id = os.urandom(16).hex()
        progress_queues[request_id] = {
            'queue': queue.Queue(),
            'timestamp': datetime.now()
        }
        return jsonify({'request_id': request_id})
    except Exception as e:
        app.logger.error(f"Error in init-progress: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/progress/<request_id>')
def progress(request_id):
    try:
        if request_id not in progress_queues:
            app.logger.error(f"Invalid request ID: {request_id}")
            return jsonify({'error': 'Invalid request ID'}), 404
        
        def generate():
            try:
                queue_data = progress_queues[request_id]
                current_queue = queue_data['queue']
                
                while True:
                    try:
                        progress_data = current_queue.get(timeout=60)
                        
                        # Debug print the progress data
                        print(f"Sending progress data: {progress_data}")
                        
                        # Ensure all fields are present
                        if isinstance(progress_data, dict):
                            if 'details' in progress_data and isinstance(progress_data['details'], dict):
                                # Make sure title is included in the details
                                if 'title' not in progress_data['details']:
                                    progress_data['details']['title'] = None
                        
                        yield f"data: {json.dumps(progress_data)}\n\n"
                        
                        if progress_data.get('status') in ['complete', 'error']:
                            if request_id in progress_queues:
                                del progress_queues[request_id]
                            break
                    except queue.Empty:
                        if request_id in progress_queues:
                            del progress_queues[request_id]
                        break
            except GeneratorExit:
                if request_id in progress_queues:
                    del progress_queues[request_id]
            except Exception as e:
                app.logger.error(f"Error in generate: {str(e)}")
                if request_id in progress_queues:
                    del progress_queues[request_id]
        
        response = Response(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['X-Accel-Buffering'] = 'no'
        return response
    except Exception as e:
        app.logger.error(f"Error in progress endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            app.logger.error("No file part in request")
            return jsonify({'error': 'No file part'}), 400
        
        request_id = request.form.get('request_id')
        if not request_id:
            app.logger.error("No request_id in form data")
            return jsonify({'error': 'No request ID provided'}), 400
            
        if request_id not in progress_queues:
            app.logger.error(f"Invalid request ID: {request_id}")
            return jsonify({'error': 'Invalid request ID'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type'}), 400

        queue_data = progress_queues[request_id]
        current_queue = queue_data['queue']

        def update_progress(message, percentage, details=None):
            if request_id in progress_queues:
                progress_data = {
                    'message': message,
                    'percentage': percentage,
                    'status': 'processing'
                }
                
                # If we have details, merge them directly into the progress data
                if details:
                    progress_data.update(details)
                
                current_queue.put(progress_data)

        # Save the uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(temp_path)

        update_progress("File uploaded successfully", 10)

        # Initialize OptimizedProductFinder with configuration
        finder = ProductFinder(
            app.config['CREDENTIALS_PATH'],
            max_concurrent_requests=app.config['MAX_CONCURRENT_REQUESTS'],
            cache_ttl=app.config['CACHE_TTL']
        )
        update_progress("Initializing product finder", 20)
        
        # Create a temporary file for CSV output
        csv_output = os.path.join(app.config['UPLOAD_FOLDER'], f'filtered_{filename}.csv')
        
        try:
            # Find product
            title = finder.find_product_and_export(
                spreadsheet_id=app.config['SPREADSHEET_ID'],
                sheet_name=app.config['SHEET_NAME'],
                local_image_path=temp_path,
                output_csv_path=csv_output,
                hash_threshold=0,
                progress_callback=update_progress
            )

            # Clean up the temporary image file
            if os.path.exists(temp_path):
                os.remove(temp_path)

            # Send completion message
            current_queue.put({
                'message': "Processing complete",
                'percentage': 100,
                'status': 'complete'
            })

            if title:
                if not os.path.exists(csv_output):
                    app.logger.error(f"CSV file not found: {csv_output}")
                    return jsonify({
                        'success': False,
                        'error': 'CSV file was not generated'
                    }), 500

                return jsonify({
                    'success': True,
                    'title': title,
                    'csv_filename': f'filtered_{filename}.csv'
                })
            else:
                if os.path.exists(csv_output):
                    os.remove(csv_output)
                return jsonify({
                    'success': False,
                    'error': 'No matching product found'
                })

        except Exception as e:
            app.logger.error(f"Error during product finding: {str(e)}")
            app.logger.error(traceback.format_exc())
            
            # Clean up temp files
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            if request_id in progress_queues:
                current_queue.put({
                    'message': f"Error: {str(e)}",
                    'percentage': None,
                    'status': 'error'
                })
            
            return jsonify({'error': str(e)}), 500

    except Exception as e:
        app.logger.error(f"Error in upload: {str(e)}")
        app.logger.error(traceback.format_exc())
        
        if request_id in progress_queues:
            current_queue.put({
                'message': f"Error: {str(e)}",
                'percentage': None,
                'status': 'error'
            })
        
        return jsonify({'error': str(e)}), 500

@app.route('/cleanup', methods=['POST'])
def cleanup():
    try:
        # Clear all caches
        for queue_data in progress_queues.values():
            queue_data['queue'].queue.clear()
        progress_queues.clear()
        
        # Clear ProductFinder caches if instance exists
        finder = ProductFinder(app.config['CREDENTIALS_PATH'])
        finder.image_hash_cache.clear()
        finder.sheet_data_cache.clear()
        
        # Force garbage collection
        gc.collect()
        
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"Error during cleanup: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/download/<filename>')
def download_file(filename):
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        # Check if handle is provided in query parameters
        handle = request.args.get('handle')
        if handle:
            # Create a ProductFinder instance
            finder = ProductFinder(app.config['CREDENTIALS_PATH'])
            
            # Directly use the source spreadsheet for filtering
            filtered_filename = f'filtered_handle_{handle}_{filename}'
            filtered_path = os.path.join(app.config['UPLOAD_FOLDER'], filtered_filename)
            
            # Filter directly from the Google Sheet
            finder.filter_sheet_by_handle(
                spreadsheet_id=app.config['SPREADSHEET_ID'], 
                sheet_name=app.config['SHEET_NAME'],
                handle=handle, 
                output_csv_path=filtered_path
            )
            
            # Update file path to the filtered file
            file_path = filtered_path
            filename = filtered_filename

        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
    except Exception as e:
        app.logger.error(f"Error in download: {str(e)}")
        app.logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    
if __name__ == '__main__':
    # Enable logging
    import logging
    logging.basicConfig(level=logging.INFO)
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 90)))