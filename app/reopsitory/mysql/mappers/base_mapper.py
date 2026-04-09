class BaseMapper:
    @staticmethod
    def to_entity(model, entity_cls):
        return entity_cls(**model.__dict__)

    @staticmethod
    def to_model(entity, model_cls):
        return model_cls(**entity.__dict__)