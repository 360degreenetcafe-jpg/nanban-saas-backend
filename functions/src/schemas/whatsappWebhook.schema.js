const { z } = require("zod");

const TextMessageSchema = z.object({
  from: z.string().min(6),
  id: z.string().min(5),
  timestamp: z.string().min(1),
  type: z.literal("text"),
  text: z.object({
    body: z.string().min(1).max(4096)
  })
}).strict();

const ButtonMessageSchema = z.object({
  from: z.string().min(6),
  id: z.string().min(5),
  timestamp: z.string().min(1),
  type: z.literal("button"),
  button: z.object({
    text: z.string().min(1).max(256).optional(),
    payload: z.string().min(1).max(256).optional()
  }).strict()
}).strict();

const InteractiveMessageSchema = z.object({
  from: z.string().min(6),
  id: z.string().min(5),
  timestamp: z.string().min(1),
  type: z.literal("interactive"),
  interactive: z.object({
    type: z.enum(["button_reply", "list_reply"]),
    button_reply: z.object({
      id: z.string().min(1).max(256),
      title: z.string().min(1).max(256)
    }).strict().optional(),
    list_reply: z.object({
      id: z.string().min(1).max(256),
      title: z.string().min(1).max(256),
      description: z.string().max(256).optional()
    }).strict().optional()
  }).strict()
}).strict();

const MessageSchema = z.union([
  TextMessageSchema,
  ButtonMessageSchema,
  InteractiveMessageSchema
]);

const WebhookChangeSchema = z.object({
  field: z.string().min(1),
  value: z.object({
    messaging_product: z.literal("whatsapp").optional(),
    metadata: z.object({
      phone_number_id: z.string().optional(),
      display_phone_number: z.string().optional()
    }).partial().optional(),
    contacts: z.array(z.any()).optional(),
    messages: z.array(MessageSchema).optional(),
    statuses: z.array(z.any()).optional()
  }).passthrough()
}).strict();

const WebhookEntrySchema = z.object({
  id: z.string().min(1).optional(),
  changes: z.array(WebhookChangeSchema).min(1)
}).strict();

const WhatsAppWebhookBodySchema = z.object({
  object: z.literal("whatsapp_business_account"),
  entry: z.array(WebhookEntrySchema).min(1)
}).strict();

function parseWebhookBody(input) {
  return WhatsAppWebhookBodySchema.parse(input);
}

module.exports = {
  WhatsAppWebhookBodySchema,
  parseWebhookBody
};
