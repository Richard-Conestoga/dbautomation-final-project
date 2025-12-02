# MongoDB Collections

## service_requests

- Database: nyc311
- Collection: service_requests
- Document shape:
  - _id: NumberLong (unique_key from MySQL)
  - created_date: Date
  - closed_date: Date or null
  - agency: String
  - complaint_type: String
  - descriptor: String
  - borough: String
  - latitude: Double
  - longitude: Double

Recommended indexes (create via Atlas UI for now):

- { created_date: 1 }
- { borough: 1, complaint_type: 1 }
