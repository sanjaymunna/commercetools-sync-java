package com.commercetools.sync.commons.helpers;

import com.commercetools.sync.commons.BaseSyncOptions;
import com.commercetools.sync.commons.exceptions.ReferenceResolutionException;
import com.commercetools.sync.services.TypeService;
import io.sphere.sdk.categories.CategoryDraft;
import io.sphere.sdk.models.Builder;
import io.sphere.sdk.types.CustomDraft;
import io.sphere.sdk.types.CustomFieldsDraft;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.sphere.sdk.types.CustomFieldsDraft.ofTypeIdAndJson;
import static io.sphere.sdk.utils.CompletableFutureUtils.exceptionallyCompletedFuture;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * This class is responsible for providing an abstract implementation of reference resolution on Custom CTP
 * resources for example (Categories, inventories and resource with a custom field).
 * For concrete reference resolution implementation of the CTP resources, this class should be extended.
 *
 * @param <D> the resource draft type for which reference resolution has to be done on.
 * @param <B> the resource draft builder where resolved values should be set. The builder type should correspond
 *            the {@code D} type.
 * @param <S> a subclass implementation of {@link BaseSyncOptions} that is used to allow/deny some specific options,
 *            specified by the user, on reference resolution.
 */
public abstract class CustomReferenceResolver
        <D extends CustomDraft, B extends Builder<? extends D>, S extends BaseSyncOptions>
    extends BaseReferenceResolver<D, S> {

    public static final String TYPE_DOES_NOT_EXIST = "Type with key '%s' doesn't exist.";
    private TypeService typeService;

    protected CustomReferenceResolver(@Nonnull final S options, @Nonnull final TypeService typeService) {
        super(options);
        this.typeService = typeService;
    }

    /**
     * Given a draft of {@code D} (e.g. {@link CategoryDraft}) this method attempts to resolve it's custom type
     * reference to return {@link CompletionStage} which contains a new instance of the draft with the resolved
     * custom type reference. The key of the custom type is taken from the from the id field of the reference.
     *
     * <p>The method then tries to fetch the key of the custom type, optimistically from a
     * cache. If the key is is not found, the resultant draft would remain exactly the same as the passed
     * draft (without a custom type reference resolution).
     *
     * @param draftBuilder the draft builder to resolve it's references.
     * @return a {@link CompletionStage} that contains as a result a new draft instance with resolved custom
     *         type references or, in case an error occurs during reference resolution,
     *         a {@link ReferenceResolutionException}.
     */
    protected abstract CompletionStage<B> resolveCustomTypeReference(@Nonnull B draftBuilder);

    /**
     * Given a draft of {@code D} (e.g. {@link CategoryDraft}) this method attempts to resolve it's custom type
     * reference to return {@link CompletionStage} which contains a new instance of the draft with the resolved
     * custom type reference. The key of the custom type is taken from the from the id field of the reference.
     *
     * <p>The method then tries to fetch the key of the custom type, optimistically from a
     * cache. If the key is not found, a completion stage that is exceptionally completed is returned containing a
     * {@link ReferenceResolutionException}.
     *
     * @param draftBuilder the draft builder to resolve it's references.
     * @param customGetter a function to return the CustomFieldsDraft instance of the draft builder.
     * @param customSetter a function to set the CustomFieldsDraft instance of the builder and return this builder.
     * @param errorMessage the error message to inject in the {@link ReferenceResolutionException} if it occurs.
     * @return a {@link CompletionStage} that contains as a result a new draft instance with resolved custom
     *         type references or, in case an error occurs during reference resolution,
     *         a {@link ReferenceResolutionException}.
     */
    @Nonnull
    protected CompletionStage<B> resolveCustomTypeReference(
        @Nonnull final B draftBuilder,
        @Nonnull final Function<B, CustomFieldsDraft> customGetter,
        @Nonnull final BiFunction<B, CustomFieldsDraft, B> customSetter,
        @Nonnull final String errorMessage) {

        final CustomFieldsDraft custom = customGetter.apply(draftBuilder);
        if (custom != null) {
            String typeKey;
            try {
                typeKey = getKeyFromResourceIdentifier(custom.getType());
            } catch (ReferenceResolutionException exception) {
                final String exceptionMessage =
                    format("%s Reason: %s", errorMessage, exception.getMessage());
                return exceptionallyCompletedFuture(new ReferenceResolutionException(exceptionMessage, exception));
            }

            return typeService
                .fetchCachedTypeId(typeKey)
                .thenCompose(resolvedTypeIdOptional -> resolvedTypeIdOptional
                    .map(resolvedTypeId ->
                        completedFuture(
                            customSetter.apply(draftBuilder, ofTypeIdAndJson(resolvedTypeId, custom.getFields()))))
                    .orElseGet(() -> {
                        final String exceptionMessage = format(TYPE_DOES_NOT_EXIST, typeKey);
                        return exceptionallyCompletedFuture(new ReferenceResolutionException(exceptionMessage));
                    }));
        }
        return completedFuture(draftBuilder);
    }


}
