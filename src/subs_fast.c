
#include <config.h>

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <util_mosq.h>

#define IS_VALID_POINTER(p) (p || p != NULL)
#define IS_TRUE(a) (a != 0)
#define IS_FALSE(a) (a == 0)

static struct _mosquitto_subhier * g_subhier_dic = NULL;

struct _sub_token {
	struct _sub_token *next;
	char *topic;
};

static bool _topic_have_wildcard(const char* topic);
static void _copy_topic_section(struct mosquitto_db *db, char* topic_section, char* topic_key, int* topic_key_free_pos);
static void _remove_topic_section(struct mosquitto_db *db, char* topic_section, char* topic_key, int* topic_key_free_pos);

static int _sub_add_fast(struct mosquitto_db *db, struct mosquitto *context, int qos, struct _mosquitto_subhier *parent, struct _sub_token *tokens, char* topic_key, int* topic_key_free_pos);

struct _mosquitto_subhier* add_child_subhier_node(struct mosquitto_db *db, struct _mosquitto_subhier *parent, char* topic_section, char* topic_key);
static int _sub_add_leaf(struct mosquitto_db *db, struct mosquitto *context, int qos, struct _mosquitto_subhier *subhier);
static int  _sub_messages_queue_fast(struct mosquitto_db *db, struct _mosquitto_subhier *parent, struct _sub_token *cur_token_section, const char *source_id, int qos, int retain, struct mosquitto_msg_store *stored, const char* topic, char* topic_key, int* topic_key_free_pos, bool set_retain);
static void _sub_topic_tokens_free(struct _sub_token *tokens);


static int _subs_process(struct mosquitto_db *db, struct _mosquitto_subhier *hier, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store *stored, bool set_retain)
{
	int rc = 0;
	int rc2;
	int client_qos, msg_qos;
	uint16_t mid;
	struct _mosquitto_subleaf *leaf;
	bool client_retain;

	leaf = hier->subs;

	if(retain && set_retain){
#ifdef WITH_PERSISTENCE
		if(strncmp(topic, "$SYS", 4)){
			/* Retained messages count as a persistence change, but only if
			 * they aren't for $SYS. */
			db->persistence_changes++;
		}
#endif
		if(hier->retained){
			mosquitto__db_msg_store_deref(db, &hier->retained);
#ifdef WITH_SYS_TREE
			db->retained_count--;
#endif
		}
		if(stored->payloadlen){
			hier->retained = stored;
			hier->retained->ref_count++;
#ifdef WITH_SYS_TREE
			db->retained_count++;
#endif
		}else{
			hier->retained = NULL;
		}
	}
	
	while(source_id && leaf){
		if(!leaf->context->id || (leaf->context->is_bridge && !strcmp(leaf->context->id, source_id))){
			leaf = leaf->next;
			continue;
		}
		/* Check for ACL topic access. */
		rc2 = mosquitto_acl_check(db, leaf->context, topic, MOSQ_ACL_READ);
		if(rc2 == MOSQ_ERR_ACL_DENIED){
			leaf = leaf->next;
			continue;
		}else if(rc2 == MOSQ_ERR_SUCCESS){
			client_qos = leaf->qos;

			if(db->config->upgrade_outgoing_qos){
				msg_qos = client_qos;
			}else{
				if(qos > client_qos){
					msg_qos = client_qos;
				}else{
					msg_qos = qos;
				}
			}
			if(msg_qos){
				mid = _mosquitto_mid_generate(leaf->context);
			}else{
				mid = 0;
			}
			if(leaf->context->is_bridge){
				/* If we know the client is a bridge then we should set retain
				 * even if the message is fresh. If we don't do this, retained
				 * messages won't be propagated. */
				client_retain = retain;
			}else{
				/* Client is not a bridge and this isn't a stale message so
				 * retain should be false. */
				client_retain = false;
			}
			
			if(mqtt3_db_message_insert(db, leaf->context, mid, mosq_md_out, msg_qos, client_retain, stored) == 1) rc = 1;
		}else{
			return 1; /* Application error */
		}
		leaf = leaf->next;
	}
	return rc;
}

static struct _sub_token *_sub_topic_append(struct _sub_token **tail, struct _sub_token **topics, char *topic)
{
	struct _sub_token *new_topic;

	if(!topic){
		return NULL;
	}
	new_topic = _mosquitto_malloc(sizeof(struct _sub_token));
	if(!new_topic){
		_mosquitto_free(topic);
		return NULL;
	}
	new_topic->next = NULL;
	new_topic->topic = topic;

	if(*tail){
		(*tail)->next = new_topic;
		*tail = (*tail)->next;
	}else{
		*topics = new_topic;
		*tail = new_topic;
	}
	return new_topic;
}

static int _sub_topic_tokenise(const char *subtopic, struct _sub_token **topics)
{
	struct _sub_token *new_topic, *tail = NULL;
	int len;
	int start, stop, tlen;
	int i;
	char *topic;

	assert(subtopic);
	assert(topics);

	if(subtopic[0] != '$'){
		new_topic = _sub_topic_append(&tail, topics, _mosquitto_strdup(""));
		if(!new_topic) goto cleanup;
	}

	len = strlen(subtopic);

	if(subtopic[0] == '/'){
		new_topic = _sub_topic_append(&tail, topics, _mosquitto_strdup(""));
		if(!new_topic) goto cleanup;

		start = 1;
	}else{
		start = 0;
	}

	stop = 0;
	for(i=start; i<len+1; i++){
		if(subtopic[i] == '/' || subtopic[i] == '\0'){
			stop = i;

			if(start != stop){
				tlen = stop-start;

				topic = _mosquitto_malloc(tlen+1);
				if(!topic) goto cleanup;
				memcpy(topic, &subtopic[start], tlen);
				topic[tlen] = '\0';
			}else{
				topic = _mosquitto_strdup("");
				if(!topic) goto cleanup;
			}
			new_topic = _sub_topic_append(&tail, topics, topic);
			if(!new_topic) goto cleanup;
			start = i+1;
		}
	}

	return MOSQ_ERR_SUCCESS;

cleanup:
	tail = *topics;
	*topics = NULL;
	while(tail){
		if(tail->topic) _mosquitto_free(tail->topic);
		new_topic = tail->next;
		_mosquitto_free(tail);
		tail = new_topic;
	}
	return 1;
}

static void _sub_topic_tokens_free(struct _sub_token *tokens)
{
	struct _sub_token *tail;

	while(tokens){
		tail = tokens->next;
		if(tokens->topic){
			_mosquitto_free(tokens->topic);
		}
		_mosquitto_free(tokens);
		tokens = tail;
	}
}
static int _sub_add_leaf(struct mosquitto_db *db, struct mosquitto *context, int qos, struct _mosquitto_subhier *subhier)
{
	struct _mosquitto_subleaf *leaf, *last_leaf;
	struct _mosquitto_subhier **subs;
	int i;
	if(!IS_VALID_POINTER(subhier))
		return MOSQ_ERR_INVAL;
	if(!IS_VALID_POINTER(context) || !IS_VALID_POINTER(context->id))
		return MOSQ_ERR_SUCCESS;

	leaf = subhier->subs;
	last_leaf = NULL;
	while(leaf){
		if(leaf->context && leaf->context->id && !strcmp(leaf->context->id, context->id)){
			/* Client making a second subscription to same topic. Only
			* need to update QoS. Return -1 to indicate this to the
			* calling function. */
			leaf->qos = qos;
			if(context->protocol == mosq_p_mqtt31){
				return -1;
			}else{
				/* mqttv311 requires retained messages are resent on
				* resubscribe. */
				return 0;
			}
		}
		last_leaf = leaf;
		leaf = leaf->next;
	}
	leaf = _mosquitto_malloc(sizeof(struct _mosquitto_subleaf));
	if(!leaf) return MOSQ_ERR_NOMEM;
	leaf->next = NULL;
	leaf->context = context;
	leaf->qos = qos;
	for(i=0; i<context->sub_count; i++){
		if(!context->subs[i]){
			context->subs[i] = subhier;
			break;
		}
	}
	if(i == context->sub_count){
		subs = _mosquitto_realloc(context->subs, sizeof(struct _mosquitto_subhier *)*(context->sub_count + 1));
		if(!subs){
			_mosquitto_free(leaf);
			return MOSQ_ERR_NOMEM;
		}
		context->subs = subs;
		context->sub_count++;
		context->subs[context->sub_count-1] = subhier;
	}
	if(last_leaf){
		last_leaf->next = leaf;
		leaf->prev = last_leaf;
	}else{
		subhier->subs = leaf;
		leaf->prev = NULL;
	}
#ifdef WITH_SYS_TREE
	db->subscription_count++;
#endif
	return MOSQ_ERR_SUCCESS;

}

// Adding a subhier node in current sub tree layer
struct _mosquitto_subhier* add_child_subhier_node(struct mosquitto_db *db, struct _mosquitto_subhier *parent, char* topic_section, char* topic_key)
{
	struct _mosquitto_subhier* cur_subhier_node = NULL;
	if(!IS_VALID_POINTER(topic_section) || !IS_VALID_POINTER(topic_key)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[add_child_subhier_node] invalid parameter! topic:(%p), topic key(%p)", topic_section, topic_key);
		return NULL;
	}
		
	HASH_FIND(hh, g_subhier_dic, topic_key, strlen(topic_key), cur_subhier_node);
	if(IS_VALID_POINTER(cur_subhier_node)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[add_child_subhier_node] subhier node is already in hash table! topic:(%p), topic key(%p)", topic_section, topic_key);
		return cur_subhier_node;
	}

	cur_subhier_node = (struct _mosquitto_subhier*)_mosquitto_calloc(1, sizeof(struct _mosquitto_subhier));
	if(!IS_VALID_POINTER(cur_subhier_node)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[add_child_subhier_node] Error: Out of memory. for subhier node!");
		return NULL;
	}
	cur_subhier_node->topic = _mosquitto_strdup(topic_section);
	if(!IS_VALID_POINTER(cur_subhier_node->topic)){
		_mosquitto_free(cur_subhier_node);
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[add_child_subhier_node] Error: Out of memory. for topic!");
		return NULL;
	}
	cur_subhier_node->subs = NULL;
	cur_subhier_node->children = NULL;
	cur_subhier_node->retained = NULL;
	cur_subhier_node->topic_key = _mosquitto_strdup(topic_key);
	if(!IS_VALID_POINTER(cur_subhier_node->topic_key)){
		_mosquitto_free(cur_subhier_node->topic);
		_mosquitto_free(cur_subhier_node);
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[add_child_subhier_node] Error: Out of memory. for topic_key!");
		return NULL;
	}
//insert current subhier node into the first node of subhier's or db-subs's child list
	cur_subhier_node->prev = NULL;
	if(IS_VALID_POINTER(parent)){
		if(IS_VALID_POINTER(parent->children)){
			parent->children->prev = cur_subhier_node;
		}
		cur_subhier_node->next = parent->children;	
		parent->children = cur_subhier_node;
		cur_subhier_node->parent = parent;
	}else{
		if(IS_VALID_POINTER(db->subs.children)){
			db->subs.children->prev = cur_subhier_node;
		}
		cur_subhier_node->next = db->subs.children;
		db->subs.children = cur_subhier_node;
		cur_subhier_node->parent = &db->subs;		
	}
	HASH_ADD_KEYPTR(hh, g_subhier_dic, cur_subhier_node->topic_key, strlen(cur_subhier_node->topic_key), cur_subhier_node);
	return cur_subhier_node;
}

static void _sub_remove_leaf(struct mosquitto_db *db, struct mosquitto *context, struct _mosquitto_subhier *subhier)
{
	struct _mosquitto_subleaf *leaf;
	int i;
	
	leaf = subhier->subs;
	while(leaf){
		if(leaf->context==context){
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_sub_remove_leaf] will remove context:[%s] from subhier(topic key:%s)!", context->id, subhier->topic_key);
#ifdef WITH_SYS_TREE
			db->subscription_count--;
#endif
			if(leaf->prev){
				leaf->prev->next = leaf->next;
			}else{
				subhier->subs = leaf->next;
			}
			if(leaf->next){
				leaf->next->prev = leaf->prev;
			}
			_mosquitto_free(leaf);
	
			/* Remove the reference to the sub that the client is keeping.
			* It would be nice to be able to use the reference directly,
			* but that would involve keeping a copy of the topic string in
			* each subleaf. Might be worth considering though. */
			for(i=0; i<context->sub_count; i++){
				if(context->subs[i] == subhier){
					context->subs[i] = NULL;
					break;
				}
			}
			return ;
		}
		leaf = leaf->next;
	}
	return ;
}

//search sub branch
static int  _sub_messages_queue_fast(struct mosquitto_db *db, struct _mosquitto_subhier *parent, struct _sub_token *cur_token_section, const char *source_id, int qos, int retain, struct mosquitto_msg_store *stored, const char* topic, char* topic_key, int* topic_key_free_pos, bool set_retain)
{
	struct _mosquitto_subhier * cur_subhier_node = NULL;
	bool sr;
	int res;
	if(!IS_VALID_POINTER(topic_key)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[_sub_messages_queue_fast] parameter error! topic_key(%p)\n", topic_key);
		return MOSQ_ERR_INVAL;
	}

	sr = set_retain;
	
	//1. check "#" for current subhier layer! must do this operation at first;
	_copy_topic_section(db, "#", topic_key, topic_key_free_pos);
	cur_subhier_node = NULL; 
	HASH_FIND_STR(g_subhier_dic, topic_key, cur_subhier_node);
	if(IS_VALID_POINTER(cur_subhier_node) && !IS_VALID_POINTER(cur_subhier_node->children)){//check sub tree for "#"
		//# must at the end of subhier layer in sub tree!
		_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_sub_messages_queue_fast] Getting subhier with topic_key(%s), subhier addr:%p, subhier addr is null means nobody in this sub node! topic_key pos:%d", topic_key, cur_subhier_node, *topic_key_free_pos);
		res = _subs_process(db, cur_subhier_node, source_id, topic, qos, retain, stored, sr);
	}
	_remove_topic_section(db, "#", topic_key, topic_key_free_pos);

	//2. check if we get to the end of topic list?
	if(!IS_VALID_POINTER(cur_token_section) || !IS_VALID_POINTER(cur_token_section->topic)){
		res = _subs_process(db, parent, source_id, topic, qos, retain, stored, sr);
		//_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_sub_messages_queue_fast] Get to the end of topic sections list! handle parent topic:(%s), parent topic_key:(%s), set_retain:(%d), result:(%d)", parent->topic, parent->topic_key, sr, res);
		return res;
	}

	//3. check "+" for current topic section!
	_copy_topic_section(db, "+", topic_key, topic_key_free_pos);
	cur_subhier_node = NULL; 
	HASH_FIND_STR(g_subhier_dic, topic_key, cur_subhier_node);
	if(IS_VALID_POINTER(cur_subhier_node)){//check sub tree for "+"
		_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_sub_messages_queue_fast] For +, Getting subhier with topic_key(%s), subhier addr:%p, subhier addr is null means nobody in this sub node! topic_key pos:%d", topic_key, cur_subhier_node, *topic_key_free_pos);
		sr = false;
		res = _sub_messages_queue_fast(db, cur_subhier_node, cur_token_section->next, source_id, qos, retain, stored, topic, topic_key, topic_key_free_pos, false);
		if(MOSQ_ERR_SUCCESS != res){
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_sub_messages_queue_fast] check sub tree for \"+\"");
		}
	}
	_remove_topic_section(db, "+", topic_key, topic_key_free_pos);

	//4. check sub tree for current topic section
	cur_subhier_node = NULL;
	_copy_topic_section(db, cur_token_section->topic, topic_key, topic_key_free_pos);
	HASH_FIND_STR(g_subhier_dic, topic_key, cur_subhier_node);
	if(IS_VALID_POINTER(cur_subhier_node)){
		res = _sub_messages_queue_fast(db, cur_subhier_node, cur_token_section->next, source_id, qos, retain, stored, topic, topic_key, topic_key_free_pos, sr);
		if(MOSQ_ERR_SUCCESS != res){
			_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[_sub_messages_queue_fast] check sub tree for current topic section:(%s) fail!", cur_token_section->topic);
		}
	}
	_remove_topic_section(db, cur_token_section->topic, topic_key, topic_key_free_pos);
	return MOSQ_ERR_SUCCESS;
}

int mqtt3_sub_add_fast(struct mosquitto_db *db, struct mosquitto *context, const char *sub, int qos, struct _mosquitto_subhier *root)
{
	int rc = 0;
	/*for topic dictionary, by jason.hou 2017.03.20*/
	char* topic_key = NULL;
	int topic_key_free_pos = 0;
	struct _mosquitto_subhier *cur_layer_subhier = NULL;
	struct _sub_token *tokens = NULL;

	assert(root);
	assert(sub);
	
	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[mqtt3_sub_add_fast] context:(%s), topic:(%s)", context->id, sub);
	if(_sub_topic_tokenise(sub, &tokens)) return 1;

	if(!_topic_have_wildcard(sub)){
		HASH_FIND_STR(g_subhier_dic, sub, cur_layer_subhier);
		if(IS_VALID_POINTER(cur_layer_subhier)){	
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[mqtt3_sub_add_fast] subhier hash table is working! topic:%s", sub);
			rc = _sub_add_leaf(db, context, qos, cur_layer_subhier);
			/* We aren't worried about -1 (already subscribed) return codes. */
			if(rc == -1) rc = MOSQ_ERR_SUCCESS;
			goto free_tokens_topic_key;
		}
	}
	
	topic_key = _mosquitto_calloc(1, strlen(sub)+1);
	if(!IS_VALID_POINTER(topic_key)){
		rc = MOSQ_ERR_NOMEM;
		goto free_tokens_topic_key;
	}

	if(sub[0] == '/'){/*first character of current topic is '/'   topic : /a/b/c is valid*/
		*(topic_key + topic_key_free_pos) = '/';
		++topic_key_free_pos;
	}
	
	cur_layer_subhier = root->children;/*first business tokens section is "" */
	if(!IS_VALID_POINTER(cur_layer_subhier)){
		cur_layer_subhier = add_child_subhier_node(db, NULL, tokens->topic, topic_key);
		if(!IS_VALID_POINTER(cur_layer_subhier)){
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mqtt3_sub_add_fast] Add subhier node fail!(children of root) res:%d", rc);
			rc = MOSQ_ERR_INVAL;
			goto free_tokens_topic_key;	
		}
	}
	
	rc = _sub_add_fast(db, context, qos, root, tokens, topic_key, &topic_key_free_pos);
	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[mqtt3_sub_add_fast] Adding sub context over! res:(%d)", rc);
	
	/* We aren't worried about -1 (already subscribed) return codes. */
	if(rc == -1) rc = MOSQ_ERR_SUCCESS;
free_tokens_topic_key:
	_sub_topic_tokens_free(tokens);
	if(IS_VALID_POINTER(topic_key))
		_mosquitto_free(topic_key);
	return rc;
}


static int _sub_add_fast(struct mosquitto_db *db, struct mosquitto *context, int qos, struct _mosquitto_subhier *parent, struct _sub_token *tokens, char* topic_key, int* topic_key_free_pos)
{
	int rc = 0;
	struct _mosquitto_subhier *cur_subhier_node = NULL;

	if(!IS_VALID_POINTER(parent) || !IS_VALID_POINTER(tokens) || !IS_VALID_POINTER(topic_key)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[_sub_add_fast] parameter error! parent(%p), tokens(%p), topic_key(%p)", parent, tokens, topic_key);
		return MOSQ_ERR_INVAL;
	}
	
	while(true){
		_copy_topic_section(db, tokens->topic, topic_key, topic_key_free_pos);
		cur_subhier_node = NULL; 
		HASH_FIND_STR(g_subhier_dic, topic_key, cur_subhier_node);
		if(!IS_VALID_POINTER(cur_subhier_node)){
			//We cann't find subhier node with current topic section in topic tree! and we must add it!
			cur_subhier_node = add_child_subhier_node(db, parent, tokens->topic, topic_key);
			if(!IS_VALID_POINTER(cur_subhier_node)){
				_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[_sub_add_fast] Add subhier node fail!");
				return MOSQ_ERR_INVAL;
			}
		}

		tokens = tokens->next;
		if(!IS_VALID_POINTER(tokens)){
			//Now, we reach the end of current topic list;
			rc = _sub_add_leaf(db, context, qos, cur_subhier_node);
			if(MOSQ_ERR_SUCCESS != rc){
				_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[_sub_add_fast] Adding leaf fail! res:%d", rc);
			}
			break;
		}
		parent = cur_subhier_node;
	}
	return rc;
	
}

static void _copy_topic_section(struct mosquitto_db *db, char* topic_section, char* topic_key, int* topic_key_free_pos)
{
	int topic_section_cpy_len = 0;
	int cur_pos = (*topic_key_free_pos);
	topic_section_cpy_len = strlen(topic_section);
	if(topic_section_cpy_len > 0){
		if(cur_pos > 0 && *(topic_key + cur_pos - 1) != '/' ){
			*(topic_key + cur_pos) = '/';
			++cur_pos;
		}
		strncpy(topic_key + cur_pos, topic_section, topic_section_cpy_len);
		*topic_key_free_pos = cur_pos + topic_section_cpy_len;
	}
}

/*remove last topic section from topic_key, the reverse operation as function:_copy_topic_section,
* so which has the  same parameters as function:_copy_topic_section
*/
static void _remove_topic_section(struct mosquitto_db *db, char* topic_section, char* topic_key, int* topic_key_free_pos)
{
	int remove_len = 0;
	int cur_pos = (*topic_key_free_pos);
	if(cur_pos <= 0)
		return;
	
	//_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_remove_topic_section] will reset topic_key(%s), topic_section(%s), topic_key_free_pos(%d)", topic_key, topic_section, *topic_key_free_pos);
	remove_len = strlen(topic_section);
	if(remove_len <= cur_pos){
		cur_pos = cur_pos - remove_len;
		memset(topic_key + cur_pos, '\0', remove_len);
		
		if((cur_pos > 0) && *(topic_key + cur_pos - 1) == '/' ){
			*(topic_key + cur_pos - 1) = '\0';
			cur_pos = cur_pos -1;
		}
	}
	(*topic_key_free_pos) = cur_pos;
	//_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[_remove_topic_section] after reset topic_key(%s), topic_section(%s), topic_key_free_pos(%d)", topic_key, topic_section, *topic_key_free_pos);
}


int mqtt3_sub_remove_fast(struct mosquitto_db *db, struct mosquitto *context, const char *sub, struct _mosquitto_subhier *root)
{
	struct _mosquitto_subhier *subhier = NULL;
	struct _mosquitto_subhier *parent = NULL;

	assert(root);
	assert(sub);
	if(!IS_VALID_POINTER(sub)){
		return 1;
	}
	HASH_FIND_STR(g_subhier_dic, sub, subhier);
	if(IS_VALID_POINTER(subhier)){
		_sub_remove_leaf(db, context, subhier);

		/*
		* check from end node which has current context until we find some node cann't be deleted!
		*/
		while(IS_VALID_POINTER(subhier)
				&& !IS_VALID_POINTER(subhier->children)
				&& !IS_VALID_POINTER(subhier->subs)
				&& !IS_VALID_POINTER(subhier->retained)){			
			/*delete subhier from subhier hash table   By jason.hou 2017.03.31*/
			parent = subhier->parent;

			if(IS_VALID_POINTER(subhier->next)){
				subhier->next->prev = subhier->prev;
			}
			if(IS_VALID_POINTER(subhier->prev)){
				subhier->prev->next= subhier->next;
			}else{//subhier is the first node in current layer.
				parent->children = subhier->next;
			}
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mqtt3_sub_remove_fast] will remove subhier with topic key(%s) in topic-subhier hash table", subhier->topic_key);
			HASH_DELETE(hh, g_subhier_dic, subhier);
			_mosquitto_free(subhier->topic_key);
			_mosquitto_free(subhier->topic);
			_mosquitto_free(subhier);
			subhier = parent;
			if(subhier == db->subs.children) break;
		}
	}
	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_messages_queue_fast(struct mosquitto_db *db, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store **stored)
{
	int rc = 0;
	struct _sub_token *tokens = NULL;
	char* topic_key = NULL;
	int topic_key_len = 0;
	int topic_key_free_pos = 0;
	int reserved_pos = 0;
	struct _mosquitto_subhier *cur_subhier_node = NULL;

	assert(db);
	assert(topic);
//	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[mqtt3_db_messages_queue_fast] Topic(%s), payload:%s", topic, (*stored)->payload);

	
	if(_sub_topic_tokenise(topic, &tokens)) return 1;

	/* Protect this message until we have sent it to all
	clients - this is required because websockets client calls
	mqtt3_db_message_write(), which could remove the message if ref_count==0.
	*/
	(*stored)->ref_count++;

	topic_key_len = strlen(topic)+3;//for topic: a/b/c, we need to handle: a/b/c/#
	topic_key = _mosquitto_calloc(1, topic_key_len);
	if(!IS_VALID_POINTER(topic_key)){
		_sub_topic_tokens_free(tokens);
		return MOSQ_ERR_NOMEM;
	}
	if(topic[0] == '/'){/*first character of current topic is '/'   topic : /a/b/c is valid*/
		*(topic_key + topic_key_free_pos) = '/';
		++topic_key_free_pos;
	}

	_copy_topic_section(db, tokens->topic, topic_key, &topic_key_free_pos);
	reserved_pos = topic_key_free_pos;
	cur_subhier_node = NULL; 
	HASH_FIND_STR(g_subhier_dic, topic_key, cur_subhier_node);
	if(IS_VALID_POINTER(cur_subhier_node)){
		if(retain){
			rc = _sub_add_fast(db, NULL, qos, cur_subhier_node, tokens->next, topic_key, &topic_key_free_pos);
			if(MOSQ_ERR_SUCCESS != rc){		
				_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[mqtt3_db_messages_queue_fast] Adding sub context fail! res:(%d)", rc);
				goto msg_queue_end;
			}
			/*reset topic_key_free_pos, cann't use function: _remove_topic_section,
			* because _sub_add_fast may add more than one topic section.
			*/
			topic_key_free_pos = reserved_pos;
			memset(topic_key + topic_key_free_pos, '\0', topic_key_len - topic_key_free_pos);
		}
		rc = _sub_messages_queue_fast(db, cur_subhier_node, tokens->next, source_id, qos, retain, *stored, topic, topic_key, &topic_key_free_pos, true);
		if(MOSQ_ERR_SUCCESS != rc){		
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[mqtt3_db_messages_queue_fast] Handle message queue fail! res:(%d)", rc);
		}
	}
	//if we cann't find subhier node in first tree layer, we will do nothing.
msg_queue_end:
	_sub_topic_tokens_free(tokens);
	_mosquitto_free(topic_key);

	/* Remove our reference and free if needed. */
	mosquitto__db_msg_store_deref(db, stored);

	return rc;
}

static bool _topic_have_wildcard(const char* topic)
{
	if(!IS_VALID_POINTER(topic))
		return false;
	
	int cur_pos = 0;
	char cur_char = *(topic + cur_pos);
	while(cur_char != '\0'){
		if((cur_char == '+') || (cur_char == '#')){
			return true;
		}
		++cur_pos;
		cur_char = *(topic + cur_pos);
	}
	return false;
}

/* Remove a subhier element, and return its parent if that needs freeing as well. */
static struct _mosquitto_subhier *tmp_remove_subs(struct _mosquitto_subhier *sub)
{
	struct _mosquitto_subhier *parent;
	struct _mosquitto_subhier *hier;
	struct _mosquitto_subhier *last = NULL;

	if(!sub || !sub->parent){
		return NULL;
	}

	if(sub->children || sub->subs){
		return NULL;
	}

	parent = sub->parent;
	hier = sub->parent->children;

	while(hier){
		if(hier == sub){
			if(last){
				last->next = hier->next;
				if(IS_VALID_POINTER(hier->next)){
					hier->next->prev = last;
				}
			}else{
				parent->children = hier->next;
				if(IS_VALID_POINTER(hier->next)){
					hier->next->prev = NULL;
				}
			}
			
			if(IS_VALID_POINTER(sub->topic_key) && strlen(sub->topic_key) >= 0){
				_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[tmp_remove_subs] will remove subhier with topic key(%s)", sub->topic_key);
				HASH_DELETE(hh, g_subhier_dic, sub);
				_mosquitto_free(sub->topic_key);
			}
			_mosquitto_free(sub->topic);
			_mosquitto_free(sub);
			break;
		}
		last = hier;
		hier = hier->next;
	}
	if(parent->subs == NULL
			&& parent->children == NULL
			&& parent->retained == NULL
			&& parent->parent){

		return parent;
	}else{
		return NULL;
	}
}


/* Remove all subscriptions for a client.
 */
int mqtt3_subs_clean_session_fast(struct mosquitto_db *db, struct mosquitto *context)
{
	int i;
	struct _mosquitto_subleaf *leaf;
	struct _mosquitto_subhier *hier;

	for(i=0; i<context->sub_count; i++){
		if(context->subs[i] == NULL){
			continue;
		}
		leaf = context->subs[i]->subs;
		while(leaf){
			if(leaf->context==context){
#ifdef WITH_SYS_TREE
				db->subscription_count--;
#endif
				if(leaf->prev){
					leaf->prev->next = leaf->next;
				}else{
					context->subs[i]->subs = leaf->next;
				}
				if(leaf->next){
					leaf->next->prev = leaf->prev;
				}
				_mosquitto_free(leaf);
				break;
			}
			leaf = leaf->next;
		}
		if(context->subs[i]->subs == NULL
				&& context->subs[i]->children == NULL
				&& context->subs[i]->retained == NULL
				&& context->subs[i]->parent){

			hier = context->subs[i];
			context->subs[i] = NULL;
			do{
				hier = tmp_remove_subs(hier);
			}while(hier);
		}
	}
	_mosquitto_free(context->subs);
	context->subs = NULL;
	context->sub_count = 0;

	return MOSQ_ERR_SUCCESS;
}

void clear_sub_hash()
{
	struct _mosquitto_subhier *sub_node, * tmp_sub_node;
	HASH_ITER(hh, g_subhier_dic, sub_node, tmp_sub_node){
		HASH_DELETE(hh, g_subhier_dic, sub_node);
	}
}

