db = db.getSiblingDB('booking');
db.slots.updateOne({_id:'slot-1'}, {$setOnInsert:{capacity:100, booked:0, version:'1'}}, {upsert:true});
db.bookings.createIndex({idempotencyKey:1}, {unique:true});
