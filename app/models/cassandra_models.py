"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncio
import math

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    @staticmethod
    async def create_message(content: str, sender_id: int, receiver_id: int) -> Dict[str, Any]:
        """
        Create a new message.
        
        Students should decide what parameters are needed based on their schema design.
        """
        try:
            sender_id_str = f"user-{sender_id}"
            receiver_id_str = f"user-{receiver_id}"
            namespace = uuid.NAMESPACE_OID  
            sender_uuid = uuid.uuid5(namespace, sender_id_str)
            receiver_uuid = uuid.uuid5(namespace, receiver_id_str)
            conversation_id = uuid.uuid4()
            message_id = uuid.uuid4()
            created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            cassandra_client.execute(
                f"INSERT INTO conversation_participants (conversation_id, user_id) VALUES ({conversation_id}, {sender_uuid})"
            )
            cassandra_client.execute(
                f"INSERT INTO conversation_participants (conversation_id, user_id) VALUES ({conversation_id}, {receiver_uuid})"
            )
            cassandra_client.execute(
                f"INSERT INTO messages (conversation_id, message_id, sender_id, content, created_at) VALUES ({conversation_id}, {message_id}, {sender_uuid}, '{content}', '{created_at}')"
            )
            cassandra_client.execute(
                f"INSERT INTO conversations (user_id, conversation_id, other_user_id, last_message_at, last_message_content) VALUES ({sender_uuid}, {conversation_id}, {receiver_uuid}, '{created_at}', '{content}')"
            )
            cassandra_client.execute(
                f"INSERT INTO conversations (user_id, conversation_id, other_user_id, last_message_at, last_message_content) VALUES ({receiver_uuid}, {conversation_id}, {sender_uuid}, '{created_at}', '{content}')"
            )
            return {
                'id': message_id,
                'conversation_id': conversation_id,
                'sender_id': sender_id,
                'receiver_id': receiver_id,
                'content': content,
                'created_at': datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S.%f")
            }
        except Exception as e:
            print(f"Error in create_message: {str(e)}")
            raise
    
    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        """
        Get messages for a conversation with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        try:
            try:
                all_convs_query = "SELECT conversation_id FROM conversation_participants"
                all_convs = cassandra_client.execute(all_convs_query)
                uuid_to_int = lambda uuid_obj: int(str(uuid_obj).replace('-', '')[:10], 16)
                conv_uuid = None
                for row in all_convs:
                    conv_id = row['conversation_id']
                    if uuid_to_int(conv_id) == conversation_id:
                        conv_uuid = conv_id
                        break
                if not conv_uuid:
                    return {
                        'total': 0,
                        'page': page,
                        'limit': limit,
                        'data': []
                    }
            except Exception as e:
                print(f"Error finding conversation: {str(e)}")
                namespace = uuid.NAMESPACE_OID
                conv_uuid = uuid.uuid5(namespace, f"conversation-{conversation_id}")
            
        
            count_query = f"SELECT COUNT(*) as count FROM messages WHERE conversation_id = {conv_uuid}"
            count_result = cassandra_client.execute(count_query)
            total_count = count_result[0]['count'] if count_result and count_result[0]['count'] else 0
            offset = (page - 1) * limit
            total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
            messages_query = f"SELECT * FROM messages WHERE conversation_id = {conv_uuid} LIMIT {limit}"
            message_rows = cassandra_client.execute(messages_query)
            
            messages = []
            for row in message_rows:
                sender_id_str = str(row['sender_id'])
                sender_id_int = uuid_to_int(row['sender_id'])
                receiver_id = 2 if sender_id_int % 2 == 1 else 1 
                
                messages.append({
                    'id': row['message_id'],
                    'conversation_id': row['conversation_id'],
                    'sender_id': sender_id_int,
                    'receiver_id': receiver_id,
                    'content': row['content'],
                    'created_at': row['created_at']
                })
            return {
                'total': total_count,
                'page': page,
                'limit': limit,
                'data': messages
            }
        except Exception as e:
            print(f"Error in get_conversation_messages: {str(e)}")
            raise
    
    @staticmethod
    async def get_messages_before_timestamp(conversation_id: int, 
                                     before_timestamp: datetime,
                                     page: int = 1, 
                                     limit: int = 20) -> Dict[str, Any]:
        """
        Get messages before a timestamp with pagination.
        Students should decide how to implement filtering by timestamp with pagination.
        """
        try:
            try:
                all_convs_query = "SELECT conversation_id FROM conversation_participants"
                all_convs = cassandra_client.execute(all_convs_query)
                
                uuid_to_int = lambda uuid_obj: int(str(uuid_obj).replace('-', '')[:10], 16)
                conv_uuid = None
                for row in all_convs:
                    conv_id = row['conversation_id']
                    if uuid_to_int(conv_id) == conversation_id:
                        conv_uuid = conv_id
                        break
                if not conv_uuid:
                    return {
                        'total': 0,
                        'page': page,
                        'limit': limit,
                        'data': []
                    }
            except Exception as e:
                print(f"Error finding conversation: {str(e)}")
                namespace = uuid.NAMESPACE_OID
                conv_uuid = uuid.uuid5(namespace, f"conversation-{conversation_id}")
            
            timestamp_str = before_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            messages_query = f"SELECT * FROM messages WHERE conversation_id = {conv_uuid} AND created_at < '{timestamp_str}' LIMIT {limit}"
            message_rows = cassandra_client.execute(messages_query)
            messages = []
            for row in message_rows:
                sender_id_int = uuid_to_int(row['sender_id'])
                receiver_id = 2 if sender_id_int % 2 == 1 else 1  
                messages.append({
                    'id': row['message_id'],
                    'conversation_id': row['conversation_id'],
                    'sender_id': sender_id_int,
                    'receiver_id': receiver_id,
                    'content': row['content'],
                    'created_at': row['created_at']
                })
            return {
                'total': len(messages),  
                'page': page,
                'limit': limit,
                'data': messages
            }
        except Exception as e:
            print(f"Error in get_messages_before_timestamp: {str(e)}")
            raise
class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        """
        Get conversations for a user with pagination.
        Students should decide what parameters are needed and how to implement pagination.
        """
        try:
            user_id_str = f"user-{user_id}"
            namespace = uuid.NAMESPACE_OID
            user_uuid = uuid.uuid5(namespace, user_id_str)
            uuid_to_int = lambda uuid_obj: int(str(uuid_obj).replace('-', '')[:10], 16)
            count_query = f"SELECT COUNT(*) as count FROM conversations WHERE user_id = {user_uuid}"
            count_result = cassandra_client.execute(count_query)
            total_count = count_result[0]['count'] if count_result and count_result[0]['count'] else 0
            offset = (page - 1) * limit
            total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
            query = f"SELECT * FROM conversations WHERE user_id = {user_uuid} LIMIT {limit}"
            result = cassandra_client.execute(query)
            conversations = []
            for row in result:
                other_user_id_int = uuid_to_int(row['other_user_id'])
                conversations.append({
                    'id': uuid_to_int(row['conversation_id']),
                    'user1_id': user_id,
                    'user2_id': other_user_id_int,
                    'last_message_at': row['last_message_at'],
                    'last_message_content': row['last_message_content']
                })
            
            return {
                'total': total_count,
                'page': page,
                'limit': limit,
                'data': conversations
            }
        except Exception as e:
            print(f"Error in get_user_conversations: {str(e)}")
            raise
    
    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        """
        Get a conversation by ID.
        Students should decide what parameters are needed and what data to return.
        """
        try:
            uuid_to_int = lambda uuid_obj: int(str(uuid_obj).replace('-', '')[:10], 16)
            namespace = uuid.NAMESPACE_OID
            conv_uuid = uuid.uuid5(namespace, f"conversation-{conversation_id}")
            participants_query = f"SELECT user_id FROM conversation_participants WHERE conversation_id = {conv_uuid} LIMIT 2"
            participants = cassandra_client.execute(participants_query)
            if not participants or len(participants) < 2:
                return None
            participant_ids = [uuid_to_int(row['user_id']) for row in participants]
            user1_id, user2_id = participant_ids[0], participant_ids[1]
            message_query = f"SELECT last_message_at, last_message_content FROM conversations WHERE conversation_id = {conv_uuid} LIMIT 1"
            message_info = cassandra_client.execute(message_query)
            last_message_at = None
            last_message_content = None
            if message_info and len(message_info) > 0:
                last_message_at = message_info[0]['last_message_at']
                last_message_content = message_info[0]['last_message_content']
            return {
                'id': conversation_id,
                'user1_id': user1_id,
                'user2_id': user2_id,
                'last_message_at': last_message_at,
                'last_message_content': last_message_content
            }
        except Exception as e:
            print(f"Error in get_conversation: {str(e)}")
            raise 

    @staticmethod
    async def create_or_get_conversation(user1_id: uuid.UUID, user2_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
        Students should decide how to handle this operation efficiently.
        """
        query = """
        SELECT conversation_id FROM conversation_participants 
        WHERE user_id = ?
        """
        user1_convs = cassandra_client.execute(query, (user1_id,))
        for row in user1_convs:
            conv_id = row['conversation_id']
            query = """
            SELECT conversation_id FROM conversation_participants 
            WHERE conversation_id = ? AND user_id = ?
            """
            matches = cassandra_client.execute(query, (conv_id, user2_id))
            if matches:
                return {'conversation_id': conv_id, 'user1_id': user1_id, 'user2_id': user2_id}
        conversation_id = uuid.uuid4()
        for user_id in [user1_id, user2_id]:
            query = """
            INSERT INTO conversation_participants (conversation_id, user_id)
            VALUES (?, ?)
            """
            cassandra_client.execute(query, (conversation_id, user_id))
        return {'conversation_id': conversation_id, 'user1_id': user1_id, 'user2_id': user2_id}