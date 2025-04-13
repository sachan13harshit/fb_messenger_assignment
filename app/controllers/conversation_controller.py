from fastapi import HTTPException, status

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.models.cassandra_models import ConversationModel

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            result = await ConversationModel.get_user_conversations(
                user_id=user_id,
                page=page,
                limit=limit
            )
            conversations = []
            for conv_data in result['data']:
                conversations.append(ConversationResponse(
                    id=conv_data['id'],
                    user1_id=conv_data['user1_id'],
                    user2_id=conv_data['user2_id'],
                    last_message_at=conv_data['last_message_at'],
                    last_message_content=conv_data['last_message_content']
                ))
            return PaginatedConversationResponse(
                total=result['total'],
                page=result['page'],
                limit=result['limit'],
                data=conversations
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get user conversations: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            result = await ConversationModel.get_conversation(conversation_id=conversation_id)
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found"
                )
            return ConversationResponse(
                id=result['id'],
                user1_id=result['user1_id'],
                user2_id=result['user2_id'],
                last_message_at=result['last_message_at'],
                last_message_content=result['last_message_content']
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation: {str(e)}"
            ) 