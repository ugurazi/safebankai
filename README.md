# SafeBank AI Agent

SafeBank AI Agent, bankacılık ve finansal kurumlar için geliştirilmiş,
doğal dilde sorulan soruları **güvenli ve kontrollü SQL sorgularına** dönüştüren
bir yapay zekâ ajanıdır.

Projenin temel amacı; SQL bilgisi olmayan iş birimleri ve üst yönetimin,
banka verilerine **yetki bazlı, KVKK uyumlu ve hızlı** şekilde erişebilmesini sağlamaktır.

---

## Problem Tanımı

Bankalarda raporlama süreçleri genellikle şu problemleri barındırır:

- İş birimleri SQL bilmez ve veri ekiplerine bağımlıdır  
- Basit raporlar bile saatler veya günler sürebilir  
- Yetkisiz veri erişimi ve KVKK riski oluşabilir  
- Manuel kontrol süreçleri operasyonel yük yaratır  

SafeBank AI Agent, bu problemleri ortadan kaldırmak için
doğal dil → güvenli SQL yaklaşımını benimser.

---

## Çözüm Yaklaşımı

Kullanıcı, bankacılık verileriyle ilgili sorusunu doğal dilde sorar.
Sistem bu soruyu aşağıdaki adımlarla işler:

1. Soru analizi ve niyet çıkarımı  
2. Veri sözlüğü (data dictionary) üzerinden tablo/kolon doğrulama  
3. Yetki ve güvenlik kontrolleri  
4. MySQL uyumlu SQL sorgusu üretimi  
5. Sonuçların sade bir dille açıklanması  

Tüm süreç boyunca yalnızca tanımlı ve izinli alanlar kullanılır.

---

## Sistem Akışı

- Doğal dil girişi (Türkçe / İngilizce)
- Niyet analizi (metric, zaman, filtre, segment)
- Data Dictionary kontrolü
- SQL üretimi
- KVKK ve güvenlik kontrolleri
- Sorgu çalıştırma
- Sonuç ve açıklama üretimi

---

## Teknik Mimari

### Backend
- Python
- Flask
- Ollama (Local LLM)
- Pandas

### Veritabanı
- MySQL 8.0

### Altyapı
- Docker
- Docker Compose

### Veri Yönetimi
- CSV tabanlı Data Dictionary
- Kolon ve tablo bazlı kontrol mekanizması

---

## Proje Yapısı
backend/
│
├─ agent/
│ ├─ planner.py # Niyet ve plan çıkarımı
│ ├─ sql_writer.py # SQL üretimi
│ ├─ guard.py # Güvenlik ve KVKK kontrolleri
│ ├─ explainer.py # Sonuç açıklamaları
│ ├─ plan_validator.py # Sorgu doğrulama
│
├─ catalog/
│ ├─ loader.py # Data dictionary yükleme
│ ├─ retriever.py # Kolon / tablo eşleştirme
│
├─ db/
│ └─ mysql.py # MySQL bağlantı katmanı
│
├─ app.py # Flask uygulaması
├─ requirements.txt
├─ docker-compose.yml
├─ seed.sql # Demo veri
├─ data_dictionary.csv # Banka veri sözlüğü

---

## Kurulum ve Çalıştırma

### Gereksinimler
- Docker
- Docker Compose
- Python 3.10+
- Ollama (local ortamda)

### MySQL Servisini Başlatma
docker-compose up -d

Python Bağımlılıkları
pip install -r requirements.txt

Flask Uygulaması
python app.py


Örnek Kullanım
Soru:
## 31.12.2025 tarihinde özel bankacılık müşteri sayısı şube bazında kaçtır?
Sistem Çıktısı:
- İlgili tablolar ve kolonlar doğrulanır
-Özel bankacılık filtresi uygulanır
-Snapshot tarihi dikkate alınır
-Şube bazında müşteri sayıları hesaplanır
-Sonuç, kullanıcıya tablo ve açıklama olarak sunulur.
-Güvenlik ve KVKK
-Yetkisiz tablo ve kolon erişimi engellenir
-Hassas alanlar otomatik olarak işaretlenir
-Sorgular limit ve filtre kontrollerinden geçer
-SQL injection riskine karşı koruma uygulanır
-Yalnızca data dictionary’de tanımlı alanlar kullanılabilir
 ## Kullanım Alanları:
-Banka üst yönetimi raporlama
-İş birimleri için self-service analiz
-Denetim ve iç kontrol ekipleri
-Hackathon ve POC çalışmaları
-Kurumsal AI Agent entegrasyonları
-Gelecek Çalışmalar
-Grafik ve görselleştirme desteği
-Rol bazlı yetkilendirme
-Sorgu ve prompt loglama
-PDF / Excel çıktı desteği



Geliştirici
Uğur Emir Azı, Nisa Ataş
Computer Engineering
AI • FinTech • NLP • Data
