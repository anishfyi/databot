import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import openai

# Load environment variables
load_dotenv()

# Initialize Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Initialize database connection
db_url = os.environ.get("DATABASE_URL")
engine = create_engine(db_url)

# Initialize OpenAI
openai.api_key = os.environ.get("OPENAI_API_KEY")

def analyze_schema():
    """Analyze and cache the database schema"""
    with engine.connect() as conn:
        # Get all tables
        tables = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """))
        
        schema_info = {}
        for table in tables:
            # Get column information for each table
            columns = conn.execute(text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table[0]}'
            """))
            schema_info[table[0]] = {col[0]: col[1] for col in columns}
    
    return schema_info

def generate_sql_query(user_question, schema_info):
    """Generate SQL query from natural language using OpenAI"""
    prompt = f"""Given the following database schema:\n{schema_info}\n\n
        Convert this question into a SQL query:\n{user_question}\n\n
        Return only the SQL query without any explanation."""
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a SQL query generator. Generate only the SQL query without any explanation."},
            {"role": "user", "content": prompt}
        ]
    )
    
    return response.choices[0].message.content.strip()

@app.event("app_mention")
async def handle_mentions(event, say):
    """Handle mentions and respond with database query results"""
    try:
        # Extract the user's question (remove the bot mention)
        text = event.get('text', '')
        question = text.split('>', 1)[1].strip() if '>' in text else text
        
        # Get database schema
        schema_info = analyze_schema()
        
        # Generate SQL query
        sql_query = generate_sql_query(question, schema_info)
        
        # Execute query
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
        
        # Format results
        if rows:
            response = "Here's what I found:\n"
            for row in rows:
                response += str(row) + "\n"
        else:
            response = "No results found."
        
        # Reply in thread
        await say(text=response, thread_ts=event.get('ts'))
        
    except Exception as e:
        await say(text=f"Sorry, I encountered an error: {str(e)}", thread_ts=event.get('ts'))

if __name__ == "__main__":
    # Start the app
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()