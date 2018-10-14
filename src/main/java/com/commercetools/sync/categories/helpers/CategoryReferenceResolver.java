package com.commercetools.sync.categories.helpers;


import com.commercetools.sync.categories.CategorySyncOptions;
import com.commercetools.sync.commons.exceptions.ReferenceResolutionException;
import com.commercetools.sync.commons.helpers.AssetReferenceResolver;
import com.commercetools.sync.commons.helpers.CustomReferenceResolver;
import com.commercetools.sync.services.CategoryService;
import com.commercetools.sync.services.TypeService;
import io.sphere.sdk.categories.Category;
import io.sphere.sdk.categories.CategoryDraft;
import io.sphere.sdk.categories.CategoryDraftBuilder;
import io.sphere.sdk.models.AssetDraft;
import io.sphere.sdk.models.ResourceIdentifier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static com.commercetools.sync.commons.utils.CompletableFutureUtils.mapValuesToFutureOfCompletedValues;
import static io.sphere.sdk.utils.CompletableFutureUtils.exceptionallyCompletedFuture;
import static java.lang.String.format;
import static java.util.Optional.of;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public final class CategoryReferenceResolver
        extends CustomReferenceResolver<CategoryDraft, CategoryDraftBuilder, CategorySyncOptions> {
    private final AssetReferenceResolver assetReferenceResolver;
    private CategoryService categoryService;
    static final String FAILED_TO_RESOLVE_PARENT = "Failed to resolve parent reference on "
        + "CategoryDraft with key:'%s'.";
    static final String PARENT_CATEGORY_DOES_NOT_EXIST = "Parent category with key '%s' doesn't exist.";
    static final String FAILED_TO_RESOLVE_CUSTOM_TYPE = "Failed to resolve custom type reference on "
        + "CategoryDraft with key:'%s'.";

    /**
     * Takes a {@link CategorySyncOptions} instance, a {@link CategoryService} and {@link TypeService} to instantiate a
     * {@link CategoryReferenceResolver} instance that could be used to resolve the category drafts in the
     * CTP project specified in the injected {@link CategorySyncOptions} instance.
     *
     * @param options         the container of all the options of the sync process including the CTP project client
     *                        and/or configuration and other sync-specific options.
     * @param typeService     the service to fetch the custom types for reference resolution.
     * @param categoryService the service to fetch the categories for reference resolution.
     */
    public CategoryReferenceResolver(@Nonnull final CategorySyncOptions options,
                                     @Nonnull final TypeService typeService,
                                     @Nonnull final CategoryService categoryService) {
        super(options, typeService);
        this.assetReferenceResolver = new AssetReferenceResolver(options, typeService);
        this.categoryService = categoryService;
    }

    /**
     * Given a {@link CategoryDraft} this method attempts to resolve the custom type and parent category references to
     * return a {@link CompletionStage} which contains a new instance of the draft with the resolved
     * references. The keys of the references are either taken from the expanded references or
     * taken from the id field of the references.
     *
     * @param categoryDraft the categoryDraft to resolve it's references.
     * @return a {@link CompletionStage} that contains as a result a new categoryDraft instance with resolved category
     *          references or, in case an error occurs during reference resolution,
     *          a {@link ReferenceResolutionException}.
     */
    @Nonnull
    public CompletionStage<CategoryDraft> resolveReferences(@Nonnull final CategoryDraft categoryDraft) {
        return resolveCustomTypeReference(CategoryDraftBuilder.of(categoryDraft))
            .thenCompose(this::resolveParentReference)
            .thenCompose(this::resolveAssetsReferences)
            .thenApply(CategoryDraftBuilder::build);
    }

    @Nonnull
    CompletionStage<CategoryDraftBuilder> resolveAssetsReferences(
        @Nonnull final CategoryDraftBuilder categoryDraftBuilder) {

        final List<AssetDraft> categoryDraftAssets = categoryDraftBuilder.getAssets();
        if (categoryDraftAssets == null) {
            return completedFuture(categoryDraftBuilder);
        }

        return mapValuesToFutureOfCompletedValues(categoryDraftAssets,
            assetReferenceResolver::resolveReferences, toList())
            .thenApply(categoryDraftBuilder::assets);
    }

    @Override
    @Nonnull
    protected CompletionStage<CategoryDraftBuilder> resolveCustomTypeReference(
        @Nonnull final CategoryDraftBuilder draftBuilder) {

        return resolveCustomTypeReference(draftBuilder,
            CategoryDraftBuilder::getCustom,
            CategoryDraftBuilder::custom,
            format(FAILED_TO_RESOLVE_CUSTOM_TYPE, draftBuilder.getKey()));
    }


    @Nonnull
    CompletionStage<CategoryDraftBuilder> resolveParentReference(@Nonnull final CategoryDraftBuilder draftBuilder) {

        final ResourceIdentifier<Category> parent = draftBuilder.getParent();

        if (parent != null && parent.getId() == null) {
            // Only if parent id is null, check key. If not null (set to  a value) return the builder as is.

            String parentKey;

            try {
                parentKey = getParentCategoryKey(parent, format(FAILED_TO_RESOLVE_PARENT, draftBuilder.getKey()));
            } catch (ReferenceResolutionException referenceResolutionException) {
                return exceptionallyCompletedFuture(referenceResolutionException);
            }

            return fetchAndResolveParentReference(draftBuilder, parentKey);
        }
        return completedFuture(draftBuilder);
    }

    /**
     * Given a category parent resource identifier, if it is not null the method validates the id field value. If it is
     * not valid, a {@link ReferenceResolutionException} will be thrown. The validity checks are:
     * <ul>
     * <li>Checks if the id value is not null or not empty.</li>
     * </ul>
     * If the above checks pass, the id value is returned in an optional. Otherwise a
     * {@link ReferenceResolutionException} is thrown.
     *
     * <p>If the passed resource identifier is {@code null}, then an empty optional is returned.
     *
     * @param parentCategoryResourceIdentifier the category parent resource identifier. If null - an empty optional is
     *                                         returned.
     * @param referenceResolutionErrorMessage  the error message if the key was not valid.
     * @return an optional containing the id or an empty optional if there is no parent reference.
     * @throws ReferenceResolutionException thrown if the key is invalid.
     */
    @Nonnull
    private static String getParentCategoryKey(
        @Nonnull final ResourceIdentifier<Category> parentCategoryResourceIdentifier,
        @Nonnull final String referenceResolutionErrorMessage) throws ReferenceResolutionException {

        try {
            return getKeyFromResourceIdentifier(parentCategoryResourceIdentifier);
        } catch (ReferenceResolutionException referenceResolutionException) {
            final String errorMessage = format("%s Reason: %s", referenceResolutionErrorMessage,
                referenceResolutionException.getMessage());
            throw new ReferenceResolutionException(errorMessage, referenceResolutionException);
        }
    }

    /**
     * TODO
     * Given a category parent resource identifier, if it is not null the method validates the id field value. If it is
     * not valid, a {@link ReferenceResolutionException} will be thrown. The validity checks are:
     * <ul>
     * <li>Checks if the id value is not null or not empty.</li>
     * </ul>
     * If the above checks pass, the id value is returned in an optional. Otherwise a
     * {@link ReferenceResolutionException} is thrown.
     *
     * <p>If the passed resource identifier is {@code null}, then an empty optional is returned.
     * @param draft TODO AND FIXME
     * @return an optional containing the id or an empty optional if there is no parent reference.
     * @throws ReferenceResolutionException thrown if the key is invalid.
     */
    @Nonnull
    public static Optional<String> getParentCategoryKey(@Nonnull final CategoryDraft draft)
        throws ReferenceResolutionException {

        if (draft.getParent() != null) {
            final String referenceResolutionErrorMessage = format(FAILED_TO_RESOLVE_PARENT, draft.getKey());
            final String parentCategoryKey = getParentCategoryKey(draft.getParent(), referenceResolutionErrorMessage);
            return of(parentCategoryKey);
        }
        return Optional.empty();
    }

    /**
     * Given a {@link CategoryDraftBuilder} and a {@code parentCategoryKey} this method fetches the actual id of the
     * category corresponding to this key, ideally from a cache. Then it sets this id on the parent reference
     * id. If the id is not found in cache nor the CTP project, the resultant draft builder
     * would remain exactly the same as the passed category draft (without parent reference resolution).
     *
     * @param draftBuilder the category draft builder to accept resolved references values.
     * @param parentCategoryKey the parent category key of to resolve it's actual id on the draft.
     * @return a {@link CompletionStage} that contains as a result the same {@code draftBuilder} category draft builder
     *         instance with resolved parent category references or an exception.
     */
    @Nonnull
    private CompletionStage<CategoryDraftBuilder> fetchAndResolveParentReference(
        @Nonnull final CategoryDraftBuilder draftBuilder,
        @Nonnull final String parentCategoryKey) {

        return categoryService
            .fetchCachedCategoryId(parentCategoryKey)
            .thenCompose(resolvedParentIdOptional -> resolvedParentIdOptional
                .map(resolvedParentId ->
                    completedFuture(
                        draftBuilder.parent(Category.referenceOfId(resolvedParentId).toResourceIdentifier())))
                .orElseGet(() -> {
                    // This case cannot happen, as CategorySync removes the reference if it wasn't there.
                    final String mainErrorMessage = format(FAILED_TO_RESOLVE_PARENT, draftBuilder.getKey());
                    final String errorMessage =
                        format("%s Reason: %s", mainErrorMessage, format(PARENT_CATEGORY_DOES_NOT_EXIST,
                            parentCategoryKey));
                    return exceptionallyCompletedFuture(new ReferenceResolutionException(errorMessage));
                }));
    }
}
