package com.commercetools.sync.categories.helpers;

import com.commercetools.sync.categories.CategorySyncOptions;
import com.commercetools.sync.categories.CategorySyncOptionsBuilder;
import com.commercetools.sync.commons.exceptions.ReferenceResolutionException;
import com.commercetools.sync.services.CategoryService;
import com.commercetools.sync.services.TypeService;
import com.commercetools.sync.services.impl.CategoryServiceImpl;
import com.commercetools.sync.services.impl.TypeServiceImpl;
import io.sphere.sdk.categories.Category;
import io.sphere.sdk.categories.CategoryDraft;
import io.sphere.sdk.categories.CategoryDraftBuilder;
import io.sphere.sdk.categories.queries.CategoryQuery;
import io.sphere.sdk.client.SphereClient;
import io.sphere.sdk.models.AssetDraft;
import io.sphere.sdk.models.AssetDraftBuilder;
import io.sphere.sdk.models.LocalizedString;
import io.sphere.sdk.models.ResourceIdentifier;
import io.sphere.sdk.models.SphereException;
import io.sphere.sdk.queries.PagedQueryResult;
import io.sphere.sdk.types.CustomFieldsDraft;
import io.sphere.sdk.types.Type;
import io.sphere.sdk.types.queries.TypeQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.commercetools.sync.categories.CategorySyncMockUtils.getMockCategoryDraftBuilder;
import static com.commercetools.sync.categories.helpers.CategoryReferenceResolver.FAILED_TO_RESOLVE_CUSTOM_TYPE;
import static com.commercetools.sync.categories.helpers.CategoryReferenceResolver.FAILED_TO_RESOLVE_PARENT;
import static com.commercetools.sync.categories.helpers.CategoryReferenceResolver.PARENT_CATEGORY_DOES_NOT_EXIST;
import static com.commercetools.sync.commons.MockUtils.getMockTypeService;

import static com.commercetools.sync.commons.helpers.BaseReferenceResolver.BLANK_KEY_VALUE_ON_RESOURCE_IDENTIFIER;
import static com.commercetools.sync.commons.helpers.CustomReferenceResolver.TYPE_DOES_NOT_EXIST;
import static io.sphere.sdk.models.LocalizedString.ofEnglish;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CategoryReferenceResolverTest {

    private TypeService typeService;
    private CategoryService categoryService;

    private static final String CACHED_CATEGORY_ID = UUID.randomUUID().toString();
    private static final String CACHED_CATEGORY_KEY = "someKey";


    private CategoryReferenceResolver referenceResolver;

    /**
     * Sets up the services and the options needed for reference resolution.
     */
    @Before
    public void setup() {
        typeService = getMockTypeService();
        categoryService = mock(CategoryService.class);
        when(categoryService.fetchCachedCategoryId(CACHED_CATEGORY_KEY))
            .thenReturn(CompletableFuture.completedFuture(Optional.of(CACHED_CATEGORY_ID)));
        final CategorySyncOptions syncOptions = CategorySyncOptionsBuilder.of(mock(SphereClient.class)).build();
        referenceResolver = new CategoryReferenceResolver(syncOptions, typeService, categoryService);
    }

    @Test
    public void resolveAssetsReferences_WithEmptyAssets_ShouldNotResolveAssets() {
        final CategoryDraftBuilder categoryDraftBuilder =
            getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key", null, null)
                .assets(emptyList());

        final CategoryDraftBuilder resolvedBuilder = referenceResolver.resolveAssetsReferences(categoryDraftBuilder)
                                                                      .toCompletableFuture().join();

        final List<AssetDraft> resolvedBuilderAssets = resolvedBuilder.getAssets();
        assertThat(resolvedBuilderAssets).isEmpty();
    }

    @Test
    public void resolveAssetsReferences_WithNullAssets_ShouldNotResolveAssets() {
        final CategoryDraftBuilder categoryDraftBuilder =
            getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key", null, null)
                .assets(null);

        final CategoryDraftBuilder resolvedBuilder = referenceResolver.resolveAssetsReferences(categoryDraftBuilder)
                                                                      .toCompletableFuture().join();

        final List<AssetDraft> resolvedBuilderAssets = resolvedBuilder.getAssets();
        assertThat(resolvedBuilderAssets).isNull();
    }

    @Test
    public void resolveAssetsReferences_WithANullAsset_ShouldNotResolveAssets() {
        final CategoryDraftBuilder categoryDraftBuilder =
            getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key", null, null)
                .assets(singletonList(null));

        final CategoryDraftBuilder resolvedBuilder = referenceResolver.resolveAssetsReferences(categoryDraftBuilder)
                                                                      .toCompletableFuture().join();

        final List<AssetDraft> resolvedBuilderAssets = resolvedBuilder.getAssets();
        assertThat(resolvedBuilderAssets).isEmpty();
    }

    @Test
    public void resolveAssetsReferences_WithAssetReferences_ShouldResolveAssets() {
        final CustomFieldsDraft customFieldsDraft = CustomFieldsDraft
            .ofTypeKeyAndJson("assetCustomTypeKey", new HashMap<>());

        final AssetDraft assetDraft = AssetDraftBuilder.of(emptyList(), ofEnglish("assetName"))
                                                       .custom(customFieldsDraft)
                                                       .build();


        final CategoryDraftBuilder categoryDraftBuilder =
            getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key", null, null)
                .assets(singletonList(assetDraft));

        final CategoryDraftBuilder resolvedBuilder = referenceResolver.resolveAssetsReferences(categoryDraftBuilder)
                                                                      .toCompletableFuture().join();


        final List<AssetDraft> resolvedBuilderAssets = resolvedBuilder.getAssets();
        assertThat(resolvedBuilderAssets).hasSize(1);
        assertThat(resolvedBuilderAssets).allSatisfy(resolvedDraft -> {
            assertThat(resolvedDraft).isNotNull();
            assertThat(resolvedDraft.getCustom()).isNotNull();
            assertThat(resolvedDraft.getCustom().getType().getId()).isEqualTo("typeId");
        });
    }

    @Test
    public void resolveParentReference_WithExceptionOnFetch_ShouldNotResolveReferences() {
        // Preparation
        final SphereClient ctpClient = mock(SphereClient.class);
        final CategorySyncOptions categorySyncOptions = CategorySyncOptionsBuilder.of(ctpClient)
                                                                                  .build();
        final CategoryService categoryService = new CategoryServiceImpl(categorySyncOptions);

        final CompletableFuture<PagedQueryResult<Category>> futureThrowingSphereException = new CompletableFuture<>();
        futureThrowingSphereException.completeExceptionally(new SphereException("CTP error on fetch"));
        when(ctpClient.execute(any(CategoryQuery.class))).thenReturn(futureThrowingSphereException);

        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            ResourceIdentifier.ofKey("nonExistingCategoryKey"), null);

        final CategoryReferenceResolver categoryReferenceResolver = new CategoryReferenceResolver(categorySyncOptions,
            typeService, categoryService);

        // Test and assertion
        assertThat(categoryReferenceResolver.resolveParentReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(SphereException.class)
            .hasMessageContaining("CTP error on fetch");
    }

    @Test
    public void resolveParentReference_WithNonExistentParentCategory_ShouldNotResolveParentReference() {
        // Preparation
        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            ResourceIdentifier.ofKey(CACHED_CATEGORY_KEY), null);
        when(categoryService.fetchCachedCategoryId(CACHED_CATEGORY_KEY))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        // Test and assertion
        final String expectedExceptionMessage = format(FAILED_TO_RESOLVE_PARENT, categoryDraft.getKey());
        final String expectedMessageWithCause = format("%s Reason: %s", expectedExceptionMessage,
            format(PARENT_CATEGORY_DOES_NOT_EXIST, CACHED_CATEGORY_KEY));
        assertThat(referenceResolver.resolveParentReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(expectedMessageWithCause);
    }

    @Test
    public void resolveParentReference_WithEmptyKeyOnParentResId_ShouldNotResolveParentReference() {
        final CategoryDraftBuilder categoryDraft = CategoryDraftBuilder.of(ofEnglish("foo"), ofEnglish("bar"))
                                                                       .key("key")
                                                                       .parent(ResourceIdentifier.ofKey(""));

        assertThat(referenceResolver.resolveParentReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve parent reference on CategoryDraft with key:'key'. Reason: %s",
                BLANK_KEY_VALUE_ON_RESOURCE_IDENTIFIER));
    }

    @Test
    public void resolveParentReference_WithNullKeyOnParentResId_ShouldNotResolveParentReference() {
        // Preparation
        final CategoryDraftBuilder categoryDraft = CategoryDraftBuilder.of(ofEnglish("foo"), ofEnglish("bar"))
                                                                       .key("key")
                                                                       .parent(ResourceIdentifier.ofKey(null));

        // Test and assertion
        assertThat(referenceResolver.resolveParentReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve parent reference on CategoryDraft with key:'key'. Reason: %s",
                BLANK_KEY_VALUE_ON_RESOURCE_IDENTIFIER));
    }

    @Test
    public void resolveParentReference_WithNonNullIdOnParentResId_ShouldResolveParentReference() {
        // Preparation
        final String parentId = UUID.randomUUID().toString();
        final CategoryDraftBuilder categoryDraft = CategoryDraftBuilder
            .of(ofEnglish("foo"), ofEnglish("bar"))
            .key("key")
            .parent(ResourceIdentifier.ofId(parentId));

        // Test
        final CategoryDraftBuilder resolvedDraftBuilder =
            referenceResolver.resolveParentReference(categoryDraft)
                             .toCompletableFuture()
                             .join();

        // Assertion
        assertThat(resolvedDraftBuilder.getParent()).isNotNull();
        assertThat(resolvedDraftBuilder.getParent().getId()).isEqualTo(parentId);
    }

    @Test
    public void resolveParentReference_WithNonNullKeyOnParentResId_ShouldResolveParentReference() {
        // Preparation
        final CategoryDraftBuilder categoryDraft = CategoryDraftBuilder
            .of(ofEnglish("foo"), ofEnglish("bar"))
            .key("key")
            .parent(ResourceIdentifier.ofKey(CACHED_CATEGORY_KEY));

        // Test
        final CategoryDraftBuilder resolvedDraftBuilder =
            referenceResolver.resolveParentReference(categoryDraft)
                             .toCompletableFuture()
                             .join();

        // Assertion
        assertThat(resolvedDraftBuilder.getParent()).isNotNull();
        assertThat(resolvedDraftBuilder.getParent().getId()).isEqualTo(CACHED_CATEGORY_ID);
    }

    @Test
    public void resolveCustomTypeReference_WithExceptionOnCustomTypeFetch_ShouldNotResolveReferences() {
        // Preparation
        final SphereClient ctpClient = mock(SphereClient.class);
        final CategorySyncOptions categorySyncOptions = CategorySyncOptionsBuilder.of(ctpClient)
                                                                                  .build();
        final TypeService typeService = new TypeServiceImpl(categorySyncOptions);

        final CompletableFuture<PagedQueryResult<Type>> futureThrowingSphereException = new CompletableFuture<>();
        futureThrowingSphereException.completeExceptionally(new SphereException("CTP error on fetch"));
        when(ctpClient.execute(any(TypeQuery.class))).thenReturn(futureThrowingSphereException);

        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            null, CustomFieldsDraft.ofTypeKeyAndJson("customTypeId", new HashMap<>()));

        final CategoryReferenceResolver categoryReferenceResolver = new CategoryReferenceResolver(categorySyncOptions,
            typeService, categoryService);

        // Test and assertion
        assertThat(categoryReferenceResolver.resolveCustomTypeReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(SphereException.class)
            .hasMessageContaining("CTP error on fetch");
    }

    @Test
    public void resolveCustomTypeReference_WithNonExistentCustomType_ShouldCompleteExceptionally() {
        // Preparation
        final CategoryDraftBuilder categoryDraft =
            getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
                null, CustomFieldsDraft.ofTypeKeyAndJson("customTypeKey", new HashMap<>()));

        when(typeService.fetchCachedTypeId(anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        // Test and assertion
        final String expectedExceptionMessage = format(FAILED_TO_RESOLVE_CUSTOM_TYPE, categoryDraft.getKey());
        final String expectedMessageWithCause = format("%s Reason: %s", expectedExceptionMessage,
            format(TYPE_DOES_NOT_EXIST, "customTypeKey"));
        assertThat(referenceResolver.resolveCustomTypeReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(expectedMessageWithCause);
    }

    @Test
    public void resolveCustomTypeReference_WithEmptyKeyOnCustomTypeResId_ShouldCompleteExceptionally() {
        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            null, CustomFieldsDraft.ofTypeKeyAndJson("", emptyMap()));

        assertThat(referenceResolver.resolveCustomTypeReference(categoryDraft))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve custom type reference on CategoryDraft with key:'key'. Reason: %s",
                BLANK_KEY_VALUE_ON_RESOURCE_IDENTIFIER));
    }

    @Test
    public void resolveCustomTypeReference_WithNonNullIdOnCustomTypeResId_ShouldResolveCustomTypeReference() {
        // Preparation
        final String customTypeId = UUID.randomUUID().toString();
        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            null, CustomFieldsDraft.ofTypeIdAndJson(customTypeId, new HashMap<>()));

        // Test
        final CategoryDraftBuilder resolvedDraftBuilder = referenceResolver.resolveCustomTypeReference(categoryDraft)
                                                                           .toCompletableFuture().join();

        // Assertion
        assertThat(resolvedDraftBuilder.getCustom()).isNotNull();
        assertThat(resolvedDraftBuilder.getCustom().getType().getId()).isEqualTo(customTypeId);
    }

    @Test
    public void resolveCustomTypeReference_WithNonNullKeyOnCustomTypeResId_ShouldResolveCustomTypeReference() {
        // Preparation
        final CategoryDraftBuilder categoryDraft = getMockCategoryDraftBuilder(Locale.ENGLISH, "myDraft", "key",
            null, CustomFieldsDraft.ofTypeKeyAndJson("myTypeKey", new HashMap<>()));

        // Test
        final CategoryDraftBuilder resolvedDraftBuilder = referenceResolver.resolveCustomTypeReference(categoryDraft)
                                                                           .toCompletableFuture().join();

        // Assertion
        assertThat(resolvedDraftBuilder.getCustom()).isNotNull();
        assertThat(resolvedDraftBuilder.getCustom().getType().getId()).isEqualTo("typeId");
    }

    @Test
    public void resolveReferences_WithNoCustomTypeReferenceAndNoParentReference_ShouldNotResolveReferences() {
        final CategoryDraft categoryDraft = mock(CategoryDraft.class);
        when(categoryDraft.getName()).thenReturn(LocalizedString.of(Locale.ENGLISH, "myDraft"));
        when(categoryDraft.getKey()).thenReturn("key");

        final CategoryDraft referencesResolvedDraft = referenceResolver.resolveReferences(categoryDraft)
                                                                       .toCompletableFuture().join();

        assertThat(referencesResolvedDraft.getCustom()).isNull();
        assertThat(referencesResolvedDraft.getParent()).isNull();
    }
}
