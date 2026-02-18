# Base Image
FROM python:3.11-slim-buster

# Working Directory
WORKDIR /

# Copy file dependensi (requirements.txt)
COPY requirements.txt .

# Install dependensi
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh kode aplikasi
COPY . .

# Expose port yang digunakan oleh aplikasi FastAPI (biasanya 8000)
EXPOSE 8000

# Command untuk menjalankan aplikasi FastAPI menggunakan Uvicorn
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "main:app", "--bind", "0.0.0.0:8000"]
