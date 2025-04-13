

## Keyspace
```sql
CREATE KEYSPACE messenger WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
```

## Tables
### messages
```sql
CREATE TABLE messenger.messages (
  conversation_id UUID,
  message_id UUID,
  sender_id UUID,
  content TEXT,
  created_at TIMESTAMP,
  PRIMARY KEY ((conversation_id), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id ASC);
```

### conversations
```sql
CREATE TABLE messenger.conversations (
  user_id UUID,
  conversation_id UUID,
  other_user_id UUID,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY ((user_id), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```

### conversation_participants
```sql
CREATE TABLE messenger.conversation_participants (
  conversation_id UUID,
  user_id UUID,
  PRIMARY KEY ((conversation_id), user_id)
);
```

